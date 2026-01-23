package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
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

		// Always log parsed registrations (not just in debug mode)
		items := make([]string, 0, len(regs))
		for _, r := range regs {
			if strings.TrimSpace(r.Name) == "" || strings.TrimSpace(r.Backend) == "" {
				continue
			}
			items = append(items, fmt.Sprintf("%s->%s", strings.TrimSpace(r.Name), strings.TrimSpace(r.Backend)))
		}
		logging.Logf("[registry] registration parsed (remote=%s count=%d items=%s)", clientIP, len(items), strings.Join(items, ","))

		if cfg != nil && cfg.Log.Level == "debug" {
			// Also log raw Registration structs
			for i, r := range regs {
				logging.Logf("[request][debug] registration[%d]: Name=%q Backend=%q PeerID=%q PeerAddr=%q",
					i, r.Name, r.Backend, r.PeerID, r.PeerAddr)
			}
		}
		registeredCount := 0
		// Extract peer_id and peer_addr from first registration (all should have the same)
		var peerID, peerAddr string
		if len(regs) > 0 {
			peerID = regs[0].PeerID
			peerAddr = regs[0].PeerAddr
		}
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] extracted from REGISTER: peer_id=%q peer_addr=%q", peerID, peerAddr)
		}
		for _, reg := range regs {
			clientName := strings.TrimSpace(reg.Name)
			backendAddr := strings.TrimSpace(reg.Backend)
			if clientName == "" {
				continue
			}
			// Design requirement: if backend is empty, don't register (no default value)
			if backendAddr == "" {
				logging.Logf("[registry] skipping service with empty backend (remote=%s name=%q)", clientIP, clientName)
				continue
			}
			// Normalize backend address (will return empty if invalid format)
			originalBackend := backendAddr
			backendAddr = routing.NormalizeBackendAddr(backendAddr, "localhost")
			if backendAddr == "" {
				logging.Logf("[registry] ERROR: skipping service with invalid backend format (remote=%s name=%q original_backend=%q)", clientIP, clientName, originalBackend)
				continue
			}
			// Log if backend changed during normalization
			if backendAddr != originalBackend && cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[request][debug] backend normalized (name=%q original=%q normalized=%q)", clientName, originalBackend, backendAddr)
			}
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[request][debug] registering service (remote=%s name=%q backend=%q peer_id=%q peer_addr=%q)", clientIP, clientName, backendAddr, peerID, peerAddr)
			}
			s.RegisterClientByNameWithPeerInfo(clientName, clientIP, backendAddr, conn, peerID, peerAddr)
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

			// Send our LOCAL service list to the newly registered peer (bidirectional sync)
			// Design: Only sync local services, not services from other peers
			// This ensures Peer B and Peer C don't see each other's services
			// Send immediately after OK to ensure client receives it
			allServices := s.GetAllServicesExcept(clientIP)
			if len(allServices) > 0 {
				regs := make([]protocol.Registration, 0, len(allServices))
				peerID := logging.GetPeerID()
				peerAddr := s.getPeerBindAddr(cfg)
				for _, svc := range allServices {
					regs = append(regs, protocol.Registration{
						Name:    svc.Name,
						Backend: svc.Backend,
					})
				}
				syncMsg := protocol.FormatSyncWithPeerID(peerID, peerAddr, regs)
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

	// Get registration read timeout from config
	// Design doc: 30s timeout to detect peer offline
	readTimeout := 30 * time.Second
	if cfg != nil {
		readTimeout = cfg.GetRegistrationReadTimeout()
	}

	// Keep reading subsequent REGISTER, SYNC, or FORWARD lines.
	// Client sends periodic heartbeat (default 10s); no separate heartbeat protocol needed.
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

		// Check if it's a FORWARD command (request to forward traffic from another peer)
		// When Peer B sends FORWARD to Peer A, Peer A receives it here on the control connection
		// Peer A should then connect to local backend and establish DATA connection back to Peer B
		if strings.HasPrefix(line, protocol.CmdForward+":") {
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[request][debug] received FORWARD command (remote=%s line=%q)", clientIP, strings.TrimSpace(line))
			}
			proxyID, name, backendAddr, ok := protocol.ParseForwardLine(line)
			if ok {
				if cfg != nil && cfg.Log.Level == "debug" {
					logging.Logf("[request][debug] FORWARD command parsed (remote=%s proxy_id=%s name=%s backend=%s)", clientIP, proxyID, name, backendAddr)
				}
				// Check if this service is a local service first
				// If it's local, we should forward directly to the backend
				// and establish DATA connection back to the requesting peer
				serviceClient := s.clients[name] // Check clients map first (name is the key there)
				if serviceClient == nil {
					// Try to find by name in services map (might be registered with peer_id)
					for key, client := range s.services {
						if client != nil && strings.HasPrefix(key, name+"@") {
							serviceClient = client
							break
						}
					}
				}

				// Find the peer connection that sent FORWARD command
				// The FORWARD command comes through the existing control connection (conn)
				// We need to find the peer's connection info to establish DATA connection
				// PeerAddr is not used to establish new connection, but to identify the peer
				// DATA connection should be established through the existing connection mechanism

				// Find peer info from the control connection
				// The connection (conn) is the control connection that sent FORWARD
				// We can find the peer's info from peerServices or services map using this connection
				var peerConn net.Conn = conn
				var peerSvcInfo *types.PeerServices

				// Try to find peer info from peerServices map by connection
				for peerIP, peerSvc := range s.peerServices {
					if peerSvc != nil && peerSvc.Conn == conn {
						peerSvcInfo = peerSvc
						logging.Logf("[server] found peer from peerServices by conn peer_ip=%s peer_id=%s peer_addr=%s", peerIP, peerSvc.PeerID, peerSvc.PeerAddr)
						break
					}
				}

				// If not found in peerServices, try to find from services map
				if peerSvcInfo == nil {
					for key, client := range s.services {
						if client != nil && client.Conn == conn && client.IP != "local" {
							// Found a service from this connection, try to get peer info
							connRemoteIP := client.IP
							if peerSvc, exists := s.peerServices[connRemoteIP]; exists && peerSvc != nil {
								peerSvcInfo = peerSvc
								logging.Logf("[server] found peer from peerServices by service IP key=%s peer_ip=%s peer_id=%s", key, connRemoteIP, peerSvc.PeerID)
								break
							}
						}
					}
				}

				// Get peer's bind address for DATA connection
				// This is the address where the peer listens for DATA connections
				sourcePeerAddr := ""
				if peerSvcInfo != nil && peerSvcInfo.PeerAddr != "" {
					sourcePeerAddr = peerSvcInfo.PeerAddr
					logging.Logf("[server] using peer_addr=%s from peerServices peer_id=%s", sourcePeerAddr, peerSvcInfo.PeerID)
				} else if peerConn != nil && peerConn.RemoteAddr() != nil {
					// Fallback: use control connection's remote address
					// This assumes the peer's bind address is the same as control connection address
					sourcePeerAddr = peerConn.RemoteAddr().String()
					logging.Logf("[server] WARNING: using control connection address as peer_addr=%s (peer may not have sent SYNC with PeerAddr)", sourcePeerAddr)
				}

				if sourcePeerAddr == "" {
					logging.Logf("[server] ERROR: cannot determine peer connection for FORWARD request proxy_id=%s name=%s", proxyID, name)
					if s.collector != nil {
						s.collector.RecordProxyError(name, "data_dial_error")
					}
					continue // Skip this FORWARD request
				}

				// Log whether service is local or remote
				serviceType := "remote"
				if serviceClient != nil && serviceClient.IP == "local" {
					serviceType = "local"
					logging.Logf("[server] FORWARD for local service name=%s backend=%s - will forward directly to backend", name, backendAddr)
				}

				logging.Logf("[server] RECEIVED FORWARD proxy_id=%s name=%s backend=%s from_addr=%s service_type=%s", proxyID, name, backendAddr, sourcePeerAddr, serviceType)
				if cfg != nil && cfg.Log.Level == "debug" {
					logging.Logf("[request][debug] FORWARD command received (from_addr=%s proxy_id=%s name=%s backend=%s service_type=%s)", sourcePeerAddr, proxyID, name, backendAddr, serviceType)
				}

				// Validate sourcePeerAddr before calling handleForwardRequest
				if sourcePeerAddr == "" {
					logging.Logf("[server] ERROR: sourcePeerAddr is empty, cannot handle FORWARD request proxy_id=%s name=%s", proxyID, name)
					if s.collector != nil {
						s.collector.RecordProxyError(name, "data_dial_error")
					}
					continue // Skip this FORWARD request
				}

				// Handle FORWARD request: connect to backend and establish DATA connection
				// Run in goroutine to avoid blocking the control connection
				// Pass the control connection so we can use it to find the peer's connection info
				logging.Logf("[server] starting handleForwardRequest goroutine proxy_id=%s name=%s peer_addr=%s", proxyID, name, sourcePeerAddr)
				go func() {
					defer func() {
						if r := recover(); r != nil {
							logging.Logf("[server] panic in handleForwardRequest proxy_id=%s name=%s err=%v", proxyID, name, r)
						}
					}()
					s.handleForwardRequestWithConn(conn, sourcePeerAddr, proxyID, name, backendAddr, cfg)
				}()
			} else {
				logging.Logf("[server] invalid FORWARD message (remote=%s message=%q)", clientIP, line)
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

	// Extract peer_id from registrations
	var syncPeerID string
	if len(regs) > 0 {
		syncPeerID = regs[0].PeerID
		if syncPeerID == "" {
			logging.Logf("[registry] WARNING: syncPeerID is empty, will use IP as fallback (remote_peer_addr=%s)", peerIP)
		}
	} else {
		logging.Logf("[registry] WARNING: no registrations in SYNC, cannot extract peer_id (remote_peer_addr=%s)", peerIP)
	}

	logging.Logf("[registry] processing SYNC (peer_id=%s remote_peer_addr=%s count=%d conn=%v conn_info=%s)",
		syncPeerID, peerIP, len(regs), conn != nil, connInfo)

	// Use the connection that sent the SYNC message
	// This is the control connection from the peer
	peerConn := conn

	// Also try to find any active connection from this peer IP
	// This handles cases where the connection might have been registered with a different IP
	// (e.g., when connecting through a load balancer)
	if peerConn == nil {
		logging.Logf("[registry] SYNC conn is nil, searching for peer connection (peer_id=%s remote_peer_addr=%s services_count=%d)",
			syncPeerID, peerIP, len(s.services))
		for key, svc := range s.services {
			if svc != nil && svc.IP == peerIP {
				logging.Logf("[registry] found service from peer key=%s ip=%s conn=%v connected=%t", key, svc.IP, svc.Conn != nil, svc.Connected)
				if svc.Conn != nil && svc.Connected {
					peerConn = svc.Conn
					logging.Logf("[registry] using existing connection for peer_id=%s remote_peer_addr=%s", syncPeerID, peerIP)
					break
				}
			}
		}
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
			logging.Logf("[registry] trying to find connection by remote IP (peerIP=%s connRemoteIP=%s)", peerIP, connRemoteIP)
			for key, svc := range s.services {
				if svc != nil && svc.Conn == conn {
					logging.Logf("[registry] found connection match key=%s ip=%s conn=%v", key, svc.IP, svc.Conn != nil)
					peerConn = conn
					break
				}
			}
		}
	}

	if peerConn == nil {
		logging.Logf("[registry] ERROR: no connection available for peer_id=%s remote_peer_addr=%s, services will be registered but cannot be forwarded",
			syncPeerID, peerIP)
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
		logging.Logf("[registry] using connection for peer_id=%s remote_peer_addr=%s (conn=%v remote=%s)",
			syncPeerID, peerIP, peerConn != nil, remoteAddr)
	}

	// Extract peer_addr from first registration
	syncPeerAddr := ""
	if len(regs) > 0 {
		syncPeerAddr = regs[0].PeerAddr
	}

	// Step 1: Update peerServices map (store Peer B's complete service list and connection info)
	// Also store by connection's RemoteAddr IP if different from peerIP (for load balancer scenarios)
	peerSvc, exists := s.peerServices[peerIP]
	if !exists {
		peerSvc = &types.PeerServices{
			PeerIP:   peerIP,
			Services: make(map[string]*types.ServiceInfo),
			LastSync: time.Now().Unix(),
		}
		s.peerServices[peerIP] = peerSvc
	}
	// Update peer connection information (PeerID, PeerAddr, Conn)
	// This allows us to find peer's bind address when receiving FORWARD requests
	// If connection changed, update it (old connection should be closed by peer)
	if peerSvc.Conn != peerConn {
		if peerSvc.Conn != nil && peerConn != nil {
			logging.Logf("[registry] peer connection changed (peer_id=%s remote_peer_addr=%s old_conn=%v new_conn=%v)",
				syncPeerID, peerIP, peerSvc.Conn != nil, peerConn != nil)
		}
		peerSvc.Conn = peerConn
	}
	peerSvc.PeerID = syncPeerID
	peerSvc.PeerAddr = syncPeerAddr

	// Also store by connection's RemoteAddr IP if different from peerIP
	// This handles cases where peer connects through a load balancer
	if peerConn != nil && peerConn.RemoteAddr() != nil {
		connRemoteIP := ""
		if tcpAddr, ok := peerConn.RemoteAddr().(*net.TCPAddr); ok {
			connRemoteIP = tcpAddr.IP.String()
		} else {
			// Extract IP from address string (format: "ip:port")
			addrStr := peerConn.RemoteAddr().String()
			if idx := strings.LastIndex(addrStr, ":"); idx > 0 {
				connRemoteIP = addrStr[:idx]
			}
		}
		if connRemoteIP != "" && connRemoteIP != peerIP {
			logging.Logf("[registry] also storing peerServices by conn RemoteAddr IP=%s (peerIP=%s)", connRemoteIP, peerIP)
			// Store the same peerSvc under both keys for lookup flexibility
			s.peerServices[connRemoteIP] = peerSvc
		}
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

	// Step 2: Also register to services map (for routing lookup)
	// This allows the service to be found during routing
	for _, reg := range regs {
		name := strings.TrimSpace(reg.Name)
		backend := strings.TrimSpace(reg.Backend)
		if name == "" || backend == "" {
			continue
		}
		logging.Logf("[registry] registering synced service name=%q backend=%q peer_id=%q remote_peer_addr=%s peer_addr=%q conn=%v",
			name, backend, syncPeerID, peerIP, syncPeerAddr, peerConn != nil)
		s.RegisterClientByNameWithPeerInfo(name, peerIP, backend, peerConn, syncPeerID, syncPeerAddr)
	}

	logging.Logf("[registry] updated peer services (peer_id=%s remote_peer_addr=%s count=%d)", syncPeerID, peerIP, len(peerSvc.Services))

	// Step 3: Print full peerServices map structure
	// Design doc requirement: print complete peerServices map structure
	s.logPeerServicesMap()
}

// handleForwardRequestWithConn handles a FORWARD request using the existing control connection
// The control connection (conn) is the established duplex connection between peers
// We use this connection to find the peer and establish DATA connection
func (s *ProxyServer) handleForwardRequestWithConn(controlConn net.Conn, peerAddr, proxyID, name, backendAddr string, cfg *config.Config) {
	logging.Logf("[server] handling FORWARD proxy_id=%s name=%s backend=%s from_addr=%s control_conn=%v", proxyID, name, backendAddr, peerAddr, controlConn != nil)
	if cfg != nil && cfg.Log.Level == "debug" {
		controlRemote := ""
		if controlConn != nil && controlConn.RemoteAddr() != nil {
			controlRemote = controlConn.RemoteAddr().String()
		}
		logging.Logf("[request][debug] FORWARD request received (proxy_id=%s name=%s backend=%s from_addr=%s control_remote=%s)", proxyID, name, backendAddr, peerAddr, controlRemote)
	}

	dialTimeout := 5 * time.Second
	if cfg != nil {
		dialTimeout = cfg.GetDialTimeout()
	}
	logging.Logf("[server] handleForwardRequest: dialTimeout=%s proxy_id=%s cfg=%v", dialTimeout, proxyID, cfg != nil)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[request][debug] FORWARD handler started (proxy_id=%s name=%s backend=%s dial_timeout=%s)", proxyID, name, backendAddr, dialTimeout)
	}

	// Step 1: Connect to local backend
	logging.Logf("[server] connecting to backend proxy_id=%s backend=%s timeout=%s", proxyID, backendAddr, dialTimeout)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[request][debug] dialing backend (proxy_id=%s name=%s backend=%s timeout=%s)", proxyID, name, backendAddr, dialTimeout)
	}
	startBackendDial := time.Now()
	backendConn, err := net.DialTimeout("tcp", backendAddr, dialTimeout)
	backendDialDuration := time.Since(startBackendDial)
	if err != nil {
		logging.Logf("[server] backend dial failed proxy_id=%s backend=%s err=%v duration=%s", proxyID, backendAddr, err, backendDialDuration)
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] backend dial failed (proxy_id=%s name=%s backend=%s err=%v duration=%s)", proxyID, name, backendAddr, err, backendDialDuration)
		}
		if s.collector != nil {
			s.collector.RecordProxyError(name, "backend_dial_error")
		}
		return
	}
	defer backendConn.Close()
	logging.Logf("[server] backend connected proxy_id=%s backend=%s local=%s duration=%s", proxyID, backendAddr, backendConn.LocalAddr(), backendDialDuration)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[request][debug] backend connected (proxy_id=%s name=%s backend=%s local=%s duration=%s)", proxyID, name, backendAddr, backendConn.LocalAddr(), backendDialDuration)
	}

	// Step 2: Establish DATA connection back to requesting peer
	// Use the control connection's remote address to establish DATA connection
	// The peer should be listening on the same address for DATA connections
	logging.Logf("[server] checking control connection proxy_id=%s control_conn=%v", proxyID, controlConn != nil)
	if controlConn == nil {
		logging.Logf("[server] ERROR: control connection is nil, cannot establish DATA connection proxy_id=%s", proxyID)
		if s.collector != nil {
			s.collector.RecordProxyError(name, "data_dial_error")
		}
		return
	}
	if controlConn.RemoteAddr() == nil {
		logging.Logf("[server] ERROR: control connection has no RemoteAddr, cannot establish DATA connection proxy_id=%s", proxyID)
		if s.collector != nil {
			s.collector.RecordProxyError(name, "data_dial_error")
		}
		return
	}

	// Use control connection's remote address for DATA connection
	// This assumes the peer listens for DATA connections on the same address as control connection
	dataTargetAddr := controlConn.RemoteAddr().String()
	logging.Logf("[server] connecting DATA channel proxy_id=%s target_addr=%s timeout=%s (using control connection's remote address)", proxyID, dataTargetAddr, dialTimeout)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[request][debug] dialing DATA channel (proxy_id=%s name=%s target_addr=%s timeout=%s)", proxyID, name, dataTargetAddr, dialTimeout)
	}

	// Attempt to dial DATA connection using control connection's remote address
	startDial := time.Now()
	dataConn, err := net.DialTimeout("tcp", dataTargetAddr, dialTimeout)
	dialDuration := time.Since(startDial)
	if err != nil {
		logging.Logf("[server] DATA dial failed proxy_id=%s target_addr=%s err=%v duration=%s", proxyID, dataTargetAddr, err, dialDuration)
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] DATA dial failed (proxy_id=%s name=%s target_addr=%s err=%v duration=%s)", proxyID, name, dataTargetAddr, err, dialDuration)
		}
		if s.collector != nil {
			s.collector.RecordProxyError(name, "data_dial_error")
		}
		return
	}
	logging.Logf("[server] DATA dial succeeded proxy_id=%s target_addr=%s duration=%s", proxyID, dataTargetAddr, dialDuration)
	defer dataConn.Close()
	logging.Logf("[server] DATA channel connected proxy_id=%s target_addr=%s local=%s remote=%s", proxyID, dataTargetAddr, dataConn.LocalAddr(), dataConn.RemoteAddr())
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[request][debug] DATA dial succeeded (proxy_id=%s name=%s target_addr=%s local=%s remote=%s duration=%s)", proxyID, name, dataTargetAddr, dataConn.LocalAddr(), dataConn.RemoteAddr(), dialDuration)
	}

	// Step 3: Send DATA header to identify this connection
	dataHeader := protocol.FormatData(proxyID)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[request][debug] sending DATA header (proxy_id=%s name=%s header=%q)", proxyID, name, strings.TrimSpace(dataHeader))
	}
	if _, err := dataConn.Write([]byte(dataHeader)); err != nil {
		logging.Logf("[server] DATA header send failed proxy_id=%s err=%v", proxyID, err)
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] DATA header send failed (proxy_id=%s name=%s err=%v)", proxyID, name, err)
		}
		if s.collector != nil {
			s.collector.RecordProxyError(name, "data_header_error")
		}
		return
	}
	logging.Logf("[server] DATA channel established proxy_id=%s", proxyID)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[request][debug] DATA header sent (proxy_id=%s name=%s)", proxyID, name)
		logging.Logf("[request][debug] DATA channel established (proxy_id=%s name=%s local=%s remote=%s)", proxyID, name, dataConn.LocalAddr(), dataConn.RemoteAddr())
	}

	logging.Logf("[server] bridge start proxy_id=%s name=%s backend=%s", proxyID, name, backendAddr)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[request][debug] starting data bridge (proxy_id=%s name=%s backend=%s)", proxyID, name, backendAddr)
	}

	// Step 4: Bridge data in both directions
	var bytesToPeer, bytesToBackend int64
	errCh := make(chan error, 2)

	go func() {
		n, e := io.Copy(dataConn, backendConn) // backend -> peer
		bytesToPeer = n
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] backend->peer done (proxy_id=%s bytes=%d err=%v)", proxyID, n, e)
		}
		// Close write side to signal EOF to peer
		if tcpConn, ok := dataConn.(*net.TCPConn); ok {
			tcpConn.CloseWrite()
		}
		errCh <- e
	}()

	go func() {
		n, e := io.Copy(backendConn, dataConn) // peer -> backend
		bytesToBackend = n
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] peer->backend done (proxy_id=%s bytes=%d err=%v)", proxyID, n, e)
		}
		// Close write side to signal EOF to backend
		if tcpConn, ok := backendConn.(*net.TCPConn); ok {
			tcpConn.CloseWrite()
		}
		errCh <- e
	}()

	// Wait for both directions to complete
	err1 := <-errCh
	err2 := <-errCh

	logging.Logf("[server] bridge done proxy_id=%s name=%s bytes_to_peer=%d bytes_to_backend=%d", proxyID, name, bytesToPeer, bytesToBackend)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[request][debug] bridge completed (proxy_id=%s name=%s bytes_to_peer=%d bytes_to_backend=%d err1=%v err2=%v)", proxyID, name, bytesToPeer, bytesToBackend, err1, err2)
	}

	// Return first non-nil error
	if err1 != nil && err1 != io.EOF {
		logging.Logf("[server] Forward failed proxy_id=%s name=%s err=%v", proxyID, name, err1)
		if s.collector != nil {
			s.collector.RecordProxyError(name, "forward_io_error")
		}
		return
	}
	if err2 != nil && err2 != io.EOF {
		logging.Logf("[server] Forward failed proxy_id=%s name=%s err=%v", proxyID, name, err2)
		if s.collector != nil {
			s.collector.RecordProxyError(name, "forward_io_error")
		}
		return
	}

	logging.Logf("[server] Forward succeeded proxy_id=%s name=%s", proxyID, name)
	if s.collector != nil {
		s.collector.RecordForwardCommand(name)
	}
}

// getPeerBindAddr returns this peer's address for other peers to connect (for DATA connections)
// Design: Each peer can configure LOCAL_PEER_ADDR as its external address
// Priority: LOCAL_PEER_ADDR (config) > POD_IP:port > hostname:port > bind_addr
func (s *ProxyServer) getPeerBindAddr(cfg *config.Config) string {
	bindAddr := ":6443"
	if cfg != nil && cfg.Peer.BindAddr != "" {
		bindAddr = cfg.Peer.BindAddr
	}

	// Extract port
	_, port, err := net.SplitHostPort(bindAddr)
	if err != nil {
		port = "6443"
	}

	// Priority 1: LOCAL_PEER_ADDR (explicitly configured local address)
	// Design doc: "每个 Peer 都可能有一个配置，用于提供给其他 Peer 连接到自己"
	// Can be LoadBalancer, Service name, or direct IP
	if cfg != nil && cfg.Peer.LocalPeerAddr != "" {
		localAddr := cfg.Peer.LocalPeerAddr
		// Ensure LOCAL_PEER_ADDR has port, add if missing
		if !strings.Contains(localAddr, ":") {
			localAddr = net.JoinHostPort(localAddr, port)
			logging.Logf("[server] LOCAL_PEER_ADDR missing port, added: %s", localAddr)
		}
		logging.Logf("[server] using LOCAL_PEER_ADDR as peer_addr: %s", localAddr)
		return localAddr
	}

	// Priority 2: POD_IP:port (Kubernetes)
	podIP := os.Getenv("POD_IP")
	if podIP != "" {
		addr := net.JoinHostPort(podIP, port)
		logging.Logf("[server] using POD_IP as peer_addr: %s", addr)
		return addr
	}

	// Priority 3: Hostname resolution
	hostname := os.Getenv("HOSTNAME")
	if hostname == "" {
		hostname, _ = os.Hostname()
	}

	if hostname != "" {
		ips, err := net.LookupHost(hostname)
		if err == nil && len(ips) > 0 {
			addr := net.JoinHostPort(ips[0], port)
			logging.Logf("[server] using hostname IP as peer_addr: %s", addr)
			return addr
		}
		addr := net.JoinHostPort(hostname, port)
		logging.Logf("[server] using hostname as peer_addr: %s", addr)
		return addr
	}

	logging.Logf("[server] using bind_addr as peer_addr (fallback): %s", bindAddr)
	return bindAddr
}

// findPeerAddrByConn finds the peer's real bind address by connection
// Returns the PeerAddr stored during registration, not the connection's RemoteAddr
func (s *ProxyServer) findPeerAddrByConn(conn net.Conn) string {
	if conn == nil {
		logging.Logf("[server] findPeerAddrByConn: conn is nil")
		return ""
	}

	connRemote := "unknown"
	if conn.RemoteAddr() != nil {
		connRemote = conn.RemoteAddr().String()
	}

	logging.Logf("[server] findPeerAddrByConn: searching for conn=%v, total_services=%d total_peers=%d", connRemote, len(s.services), len(s.peerServices))

	// Debug: print all services to see what's registered
	for key, client := range s.services {
		if client == nil {
			continue
		}
		connMatch := "no"
		if client.Conn == conn {
			connMatch = "YES"
		}
		connInfo := "nil"
		if client.Conn != nil && client.Conn.RemoteAddr() != nil {
			connInfo = client.Conn.RemoteAddr().String()
		}
		logging.Logf("[server] findPeerAddrByConn: service key=%s peer_id=%s peer_addr=%s ip=%s conn=%s match=%s",
			key, client.PeerID, client.PeerAddr, client.IP, connInfo, connMatch)
	}

	// Debug: print all peers in peerServices map
	for peerIP, peerSvc := range s.peerServices {
		if peerSvc != nil {
			connInfo := "nil"
			if peerSvc.Conn != nil && peerSvc.Conn.RemoteAddr() != nil {
				connInfo = peerSvc.Conn.RemoteAddr().String()
			}
			connMatch := "no"
			if peerSvc.Conn == conn {
				connMatch = "YES"
			}
			logging.Logf("[server] findPeerAddrByConn: peer peer_ip=%s peer_id=%s peer_addr=%s conn=%s match=%s",
				peerIP, peerSvc.PeerID, peerSvc.PeerAddr, connInfo, connMatch)
		}
	}

	// First try: find from peerServices map by connection (most reliable)
	// peerServices map now stores Conn directly, so we can match by connection
	for peerIP, peerSvc := range s.peerServices {
		if peerSvc != nil && peerSvc.Conn == conn {
			if peerSvc.PeerAddr != "" {
				logging.Logf("[server] findPeerAddrByConn: FOUND from peerServices by conn peer_ip=%s peer_id=%s peer_addr=%s",
					peerIP, peerSvc.PeerID, peerSvc.PeerAddr)
				return peerSvc.PeerAddr
			}
		}
	}

	// Second try: find any service registered with this connection
	for key, client := range s.services {
		if client == nil {
			continue
		}

		// Check if this is the same connection
		if client.Conn == conn {
			logging.Logf("[server] findPeerAddrByConn: FOUND MATCH key=%s peer_id=%s peer_addr=%s ip=%s", key, client.PeerID, client.PeerAddr, client.IP)
			// If this is a local service, it means the connection is from a local client, not a peer
			// This shouldn't happen for FORWARD commands, but if it does, we should handle it
			if client.IP == "local" {
				logging.Logf("[server] findPeerAddrByConn: WARNING - matched local service, this connection is not from a peer. This should not happen for FORWARD commands.")
				// Return empty to indicate we couldn't find the peer address
				return ""
			}
			if client.PeerAddr != "" {
				logging.Logf("[server] found peer_addr=%s (peer_id=%s) for conn=%v", client.PeerAddr, client.PeerID, connRemote)
				return client.PeerAddr
			} else {
				logging.Logf("[server] WARNING: peer_addr is EMPTY for key=%s (peer_id=%s), this will cause DATA connection to fail", key, client.PeerID)
			}
		}
	}

	// Fallback: try to find by connection's remote IP address
	// This handles cases where the connection might not match exactly (e.g., reconnection)
	if conn != nil {
		remoteAddr := conn.RemoteAddr()
		if remoteAddr != nil {
			remoteIP := ""
			if tcpAddr, ok := remoteAddr.(*net.TCPAddr); ok {
				remoteIP = tcpAddr.IP.String()
			} else {
				// Extract IP from address string (format: "ip:port")
				addrStr := remoteAddr.String()
				if idx := strings.LastIndex(addrStr, ":"); idx > 0 {
					remoteIP = addrStr[:idx]
				}
			}
			if remoteIP != "" {
				logging.Logf("[server] findPeerAddrByConn: trying fallback by IP=%s", remoteIP)

				// First try: find from peerServices map by IP (most reliable)
				// peerServices map now stores PeerAddr directly, so we can use it
				if peerSvc, exists := s.peerServices[remoteIP]; exists && peerSvc != nil {
					if peerSvc.PeerAddr != "" {
						logging.Logf("[server] findPeerAddrByConn: FOUND from peerServices by IP=%s peer_addr=%s peer_id=%s",
							remoteIP, peerSvc.PeerAddr, peerSvc.PeerID)
						return peerSvc.PeerAddr
					}
				}

				// Second try: find any service from this peer IP (not local services, as they don't have PeerAddr)
				// This helps when the requested service is local (ip=local) but we need the peer's bind address
				// The peer that sent FORWARD should have registered other services via SYNC, which have PeerAddr
				for key, client := range s.services {
					if client == nil {
						continue
					}
					// Check if IP matches (for remote services, IP is the peer's IP)
					// Exclude local services as they don't have PeerAddr
					if client.IP == remoteIP && client.IP != "local" && client.PeerAddr != "" {
						logging.Logf("[server] findPeerAddrByConn: FOUND by IP key=%s peer_id=%s peer_addr=%s ip=%s",
							key, client.PeerID, client.PeerAddr, client.IP)
						return client.PeerAddr
					}
				}
				logging.Logf("[server] findPeerAddrByConn: no service found for IP=%s (may need to wait for SYNC from this peer)", remoteIP)
			}
		}
	}

	logging.Logf("[server] ERROR: peer_addr not found for conn=%v, will use RemoteAddr as fallback (this will likely fail)", connRemote)
	return ""
}
