package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ops-proxy/pkg/config"
	"github.com/ops-proxy/pkg/logging"
	"github.com/ops-proxy/pkg/protocol"
	"github.com/ops-proxy/pkg/routing"
	"github.com/ops-proxy/pkg/types"
)

// HandleClientRegistration handles client registration
// Supports registering multiple name:backend_addr combinations in one connection
// Protocol format:
//   - Single registration: "REGISTER:name:backend_addr"
//   - Multiple registrations: "REGISTER:name1:backend1,name2:backend2,name3:backend3"
func (s *ProxyServer) HandleClientRegistration(conn net.Conn, cfg *config.Config) {
	reader := bufio.NewReader(conn)
	remote := ""
	if conn != nil && conn.RemoteAddr() != nil {
		remote = conn.RemoteAddr().String()
	}

	// Read first line to determine connection type.
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	firstLine, err := reader.ReadString('\n')
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] registration read error (remote=%s err=%v)", remote, err)
		}
		if err.Error() != "EOF" {
			logging.Logf("Error reading registration: %v", err)
		}
		_ = conn.Close()
		return
	}

	firstLine = strings.TrimSpace(firstLine)

	// Get client IP early for logging
	clientIP := ""
	if conn != nil && conn.RemoteAddr() != nil {
		if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
			clientIP = tcpAddr.IP.String()
		} else {
			clientIP = conn.RemoteAddr().String()
		}
	}

	// DATA connection: "DATA:<proxy-id>"
	// Used as a per-request data tunnel (one request -> one data connection).
	if proxyID, ok := protocol.ParseDataLine(firstLine); ok {
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] DATA connection request (remote=%s proxy_id=%s)", clientIP, proxyID)
		}
		if proxyID == "" {
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[request][debug] invalid DATA message (remote=%s)", clientIP)
			}
			logging.Logf("Invalid DATA message")
			_ = conn.Close()
			return
		}

		s.pendingLock.Lock()
		ch := s.pendingData[proxyID]
		if ch != nil {
			delete(s.pendingData, proxyID)
		}
		s.pendingLock.Unlock()

		if ch == nil {
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[request][debug] unexpected DATA connection (remote=%s proxy_id=%s reason=no_pending_forward)", clientIP, proxyID)
			}
			logging.Logf("Unexpected DATA connection for proxy-id=%s (no pending forward)", proxyID)
			if s.collector != nil {
				s.collector.RecordDataUnexpected()
			}
			_ = conn.Close()
			return
		}

		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] DATA connection matched (remote=%s proxy_id=%s)", clientIP, proxyID)
		}
		// Hand off the connection to the waiting proxy goroutine.
		ch <- conn
		return
	}

	// Otherwise this is a control connection that should send REGISTER lines.
	defer func() {
		s.UnregisterClientsByConn(conn)
		_ = conn.Close()
	}()

	// Ensure clientIP is set for TCP connections
	if clientIP == "" {
		if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
			clientIP = tcpAddr.IP.String()
		} else {
			clientIP = conn.RemoteAddr().String()
		}
	}

	processRegister := func(message string) {
		logging.Logf("[registry] processing registration (remote=%s message=%q)", clientIP, message)
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] registration request (remote=%s message=%q)", clientIP, message)
		}
		regs, ok := protocol.ParseRegisterLine(message)
		if !ok {
			logging.Logf("[registry] invalid registration message (remote=%s message=%q)", clientIP, message)
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[request][debug] invalid registration message (remote=%s message=%q)", clientIP, message)
			}
			_, _ = conn.Write([]byte("ERROR:Invalid registration\n"))
			return
		}
		if cfg != nil && cfg.Log.Level == "debug" {
			items := make([]string, 0, len(regs))
			for _, r := range regs {
				if strings.TrimSpace(r.Name) == "" || strings.TrimSpace(r.Backend) == "" {
					continue
				}
				items = append(items, fmt.Sprintf("%s->%s", strings.TrimSpace(r.Name), strings.TrimSpace(r.Backend)))
			}
			logging.Logf("[request][debug] registration parsed (remote=%s count=%d items=%s)", clientIP, len(items), strings.Join(items, ","))
		}
		registeredCount := 0
		// Extract peer_id from first registration (all should have the same peer_id)
		var peerID string
		if len(regs) > 0 {
			peerID = regs[0].PeerID
		}
		for _, reg := range regs {
			clientName := strings.TrimSpace(reg.Name)
			backendAddr := strings.TrimSpace(reg.Backend)
			if clientName == "" {
				continue
			}
			if backendAddr == "" {
				backendAddr = net.JoinHostPort(clientIP, "80")
			} else {
				backendAddr = routing.NormalizeBackendAddr(backendAddr, "localhost")
			}
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[request][debug] registering service (remote=%s name=%q backend=%q peer_id=%q)", clientIP, clientName, backendAddr, peerID)
			}
			s.RegisterClientByNameWithPeerID(clientName, clientIP, backendAddr, conn, peerID)
			registeredCount++
		}
		if registeredCount > 0 {
			logging.Logf("[registry] registration success (remote=%s count=%d)", clientIP, registeredCount)
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[request][debug] registration success (remote=%s count=%d)", clientIP, registeredCount)
			}
			s.logServicesTable(cfg)
			okMsg := fmt.Sprintf("OK:%d\n", registeredCount)
			logging.Logf("[registry] sending acknowledgment (remote=%s msg=%q)", clientIP, strings.TrimSpace(okMsg))
			n, err := conn.Write([]byte(okMsg))
			if err != nil {
				logging.Logf("[registry] failed to send acknowledgment (remote=%s err=%v)", clientIP, err)
			} else {
				logging.Logf("[registry] acknowledgment sent (remote=%s bytes=%d)", clientIP, n)
			}

			// Send our service list to the newly registered peer (bidirectional sync)
			// Send immediately after OK to ensure client receives it
			allServices := s.GetAllServicesExcept(clientIP)
			if len(allServices) > 0 {
				regs := make([]protocol.Registration, 0, len(allServices))
				peerID := logging.GetPeerID()
				for _, svc := range allServices {
					regs = append(regs, protocol.Registration{
						Name:    svc.Name,
						Backend: svc.Backend,
					})
				}
				syncMsg := protocol.FormatSyncWithPeerID(peerID, regs)
				logging.Logf("[registry] sending service sync (remote=%s count=%d)", clientIP, len(regs))
				if cfg != nil && cfg.Log.Level == "debug" {
					logging.Logf("[request][debug] sending service sync (remote=%s count=%d)", clientIP, len(regs))
				}
				syncN, syncErr := conn.Write([]byte(syncMsg))
				if syncErr != nil {
					logging.Logf("[registry] failed to send service sync (remote=%s err=%v)", clientIP, syncErr)
				} else {
					logging.Logf("[registry] service sync sent (remote=%s bytes=%d)", clientIP, syncN)
				}
			}
		} else {
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[request][debug] registration failed (remote=%s reason=no_valid_registrations)", clientIP)
			}
			_, _ = conn.Write([]byte("ERROR:No valid registrations\n"))
		}
	}

	// Process first line (must be REGISTER)
	processRegister(firstLine)

	// Get registration read timeout from config, default 180s
	readTimeout := 180 * time.Second
	if cfg != nil {
		readTimeout = cfg.GetRegistrationReadTimeout()
	}

	// Keep reading subsequent REGISTER, SYNC, or FORWARD lines. Client sends periodic heartbeat (default 30s); no separate heartbeat protocol needed.
	for {
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		line, err := reader.ReadString('\n')
		conn.SetReadDeadline(time.Time{})
		if err != nil {
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[request][debug] registration connection closed (remote=%s err=%v)", clientIP, err)
			}
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Check if it's a FORWARD command (request to forward traffic)
		// Note: FORWARD commands are sent from server to client, not the other way around
		// If we receive a FORWARD command here, it means the client is trying to forward to us
		// This should not happen in normal operation, but we'll handle it gracefully
		if strings.HasPrefix(line, protocol.CmdForward+":") {
			proxyID, name, backendAddr, ok := protocol.ParseForwardLine(line)
			if ok {
				logging.Logf("[registry] received FORWARD command from client (remote=%s proxy_id=%s name=%s backend=%s) - this is unexpected, client should not send FORWARD", clientIP, proxyID, name, backendAddr)
				if cfg != nil && cfg.Log.Level == "debug" {
					logging.Logf("[request][debug] received FORWARD command from client (remote=%s proxy_id=%s name=%s backend=%s)", clientIP, proxyID, name, backendAddr)
				}
				// Client should not send FORWARD commands - this is a protocol error
				// But we'll just log it and continue, as the client will handle the forwarding
			} else {
				logging.Logf("[registry] invalid FORWARD message (remote=%s message=%q)", clientIP, line)
			}
		} else if strings.HasPrefix(line, protocol.CmdSync+":") {
			// Check if it's a SYNC command (service list from peer)
			regs, ok := protocol.ParseSyncLine(line)
			if ok {
				// Pass the connection that sent the SYNC message
				s.processSync(clientIP, regs, conn, cfg)
			} else {
				logging.Logf("[registry] invalid SYNC message (remote=%s message=%q)", clientIP, line)
			}
		} else {
			// Regular REGISTER command
			processRegister(line)
		}
	}
}

// processSync processes a SYNC command from a remote peer
// According to design document:
// 1. Peer A receives SYNC command
// 2. Update peerServices map (store Peer B's complete service list)
// 3. Also register to services map (for routing lookup)
// 4. Print full peerServices map structure
// conn is the control connection that sent the SYNC message
func (s *ProxyServer) processSync(peerIP string, regs []protocol.Registration, conn net.Conn, cfg *config.Config) {
	connInfo := "nil"
	if conn != nil {
		if conn.RemoteAddr() != nil {
			connInfo = conn.RemoteAddr().String()
		} else {
			connInfo = "no-remote-addr"
		}
	}
	logging.Logf("[registry] processing SYNC (peer=%s count=%d conn=%v conn_info=%s)", peerIP, len(regs), conn != nil, connInfo)

	// Step 1: Update peerServices map (store Peer B's complete service list)
	s.peerServicesLock.Lock()
	peerSvc, exists := s.peerServices[peerIP]
	if !exists {
		peerSvc = &types.PeerServices{
			PeerIP:   peerIP,
			Services: make(map[string]*types.ServiceInfo),
			LastSync: time.Now().Unix(),
		}
		s.peerServices[peerIP] = peerSvc
	}

	// Update services map in peerServices (complete replacement)
	peerSvc.Services = make(map[string]*types.ServiceInfo)
	for _, reg := range regs {
		name := strings.TrimSpace(reg.Name)
		backend := strings.TrimSpace(reg.Backend)
		if name == "" || backend == "" {
			continue
		}
		peerSvc.Services[name] = &types.ServiceInfo{
			Name:        name,
			BackendAddr: backend,
			SourcePeer:  peerIP,
		}
	}
	peerSvc.LastSync = time.Now().Unix()
	s.peerServicesLock.Unlock()

	// Use the connection that sent the SYNC message
	// This is the control connection from the peer
	peerConn := conn

	// Also try to find any active connection from this peer IP
	// This handles cases where the connection might have been registered with a different IP
	// (e.g., when connecting through a load balancer)
	if peerConn == nil {
		s.clientsLock.RLock()
		logging.Logf("[registry] SYNC conn is nil, searching for peer connection (peer=%s services_count=%d)", peerIP, len(s.services))
		for key, svc := range s.services {
			if svc != nil && svc.IP == peerIP {
				logging.Logf("[registry] found service from peer key=%s ip=%s conn=%v connected=%t", key, svc.IP, svc.Conn != nil, svc.Connected)
				if svc.Conn != nil && svc.Connected {
					peerConn = svc.Conn
					logging.Logf("[registry] using existing connection for peer=%s", peerIP)
					break
				}
			}
		}
		s.clientsLock.RUnlock()
	}

	// If still no connection, try to find by matching the connection's remote address
	// This handles cases where peerIP might be different from the actual connection IP
	if peerConn == nil && conn != nil {
		connRemoteIP := ""
		if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
			connRemoteIP = tcpAddr.IP.String()
		} else if conn.RemoteAddr() != nil {
			connRemoteIP = conn.RemoteAddr().String()
		}

		if connRemoteIP != "" && connRemoteIP != peerIP {
			s.clientsLock.RLock()
			logging.Logf("[registry] trying to find connection by remote IP (peerIP=%s connRemoteIP=%s)", peerIP, connRemoteIP)
			for key, svc := range s.services {
				if svc != nil && svc.Conn == conn {
					logging.Logf("[registry] found connection match key=%s ip=%s conn=%v", key, svc.IP, svc.Conn != nil)
					peerConn = conn
					break
				}
			}
			s.clientsLock.RUnlock()
		}
	}

	if peerConn == nil {
		logging.Logf("[registry] ERROR: no connection available for peer=%s, services will be registered but cannot be forwarded", peerIP)
		logging.Logf("[registry] DEBUG: conn parameter was %v, peerIP=%s", conn != nil, peerIP)
		if conn != nil {
			remoteAddr := ""
			if conn.RemoteAddr() != nil {
				remoteAddr = conn.RemoteAddr().String()
			}
			logging.Logf("[registry] DEBUG: conn.RemoteAddr()=%s", remoteAddr)
		}
	} else {
		remoteAddr := ""
		if peerConn.RemoteAddr() != nil {
			remoteAddr = peerConn.RemoteAddr().String()
		}
		logging.Logf("[registry] using connection for peer=%s (conn=%v remote=%s)", peerIP, peerConn != nil, remoteAddr)
	}

	// Step 2: Also register to services map (for routing lookup)
	// This allows the service to be found during routing
	// Extract peer_id from first registration (all should have the same peer_id)
	var syncPeerID string
	if len(regs) > 0 {
		syncPeerID = regs[0].PeerID
	}
	for _, reg := range regs {
		name := strings.TrimSpace(reg.Name)
		backend := strings.TrimSpace(reg.Backend)
		if name == "" || backend == "" {
			continue
		}
		logging.Logf("[registry] registering synced service name=%q backend=%q peer=%s peer_id=%q conn=%v", name, backend, peerIP, syncPeerID, peerConn != nil)
		s.RegisterClientByNameWithPeerID(name, peerIP, backend, peerConn, syncPeerID)
	}

	logging.Logf("[registry] updated peer services (peer=%s count=%d)", peerIP, len(peerSvc.Services))

	// Step 3: Print full peerServices map structure
	// Design doc requirement: print complete peerServices map structure
	s.logPeerServicesMap()
}
