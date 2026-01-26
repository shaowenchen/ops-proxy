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
	"github.com/ops-proxy/pkg/proxy"
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
		logging.Logf("[register] processing registration from %s", clientIP)
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] register message=%q", message)
		}
		regs, ok := protocol.ParseRegisterLine(message)
		if !ok {
			logging.Logf("[error] invalid registration message from %s: %q", clientIP, message)
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
		logging.Logf("[register] parsed %d service(s) from %s: %s", len(items), clientIP, strings.Join(items, ","))

		if cfg != nil && cfg.Log.Level == "debug" {
			// Also log raw Registration structs
			for i, r := range regs {
				logging.Logf("[debug] registration[%d]: name=%q backend=%q peer_id=%q peer_addr=%q",
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
			logging.Logf("[debug] extracted peer_id=%q peer_addr=%q", peerID, peerAddr)
		}
		for _, reg := range regs {
			clientName := strings.TrimSpace(reg.Name)
			backendAddr := strings.TrimSpace(reg.Backend)
			if clientName == "" {
				continue
			}
			// Design requirement: if backend is empty, don't register (no default value)
			if backendAddr == "" {
				logging.Logf("[register] skipping service %q from %s: empty backend", clientName, clientIP)
				continue
			}
			// Normalize backend address (will return empty if invalid format)
			originalBackend := backendAddr
			backendAddr = routing.NormalizeBackendAddr(backendAddr, "localhost")
			if backendAddr == "" {
				logging.Logf("[error] skipping service %q from %s: invalid backend format %q", clientName, clientIP, originalBackend)
				continue
			}
			// Log if backend changed during normalization
			if backendAddr != originalBackend && cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[debug] backend normalized: %q -> %q", originalBackend, backendAddr)
			}
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[debug] registering service name=%q backend=%q peer_id=%q peer_addr=%q", clientName, backendAddr, peerID, peerAddr)
			}
			s.RegisterClientByNameWithPeerInfo(clientName, clientIP, backendAddr, conn, peerID, peerAddr)
			registeredCount++
		}
		if registeredCount > 0 {
			logging.Logf("[register] success: registered %d service(s) from %s", registeredCount, clientIP)
			s.logServicesTable(cfg)
			okMsg := fmt.Sprintf("OK:%d\n", registeredCount)
			n, err := conn.Write([]byte(okMsg))
			if err != nil {
				logging.Logf("[error] failed to send OK to %s: %v", clientIP, err)
			} else if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[debug] sent OK:%d to %s (%d bytes)", registeredCount, clientIP, n)
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
				logging.Logf("[sync] sending %d local service(s) to %s", len(regs), clientIP)
				syncN, syncErr := conn.Write([]byte(syncMsg))
				if syncErr != nil {
					logging.Logf("[error] failed to send SYNC to %s: %v", clientIP, syncErr)
				} else if cfg != nil && cfg.Log.Level == "debug" {
					logging.Logf("[debug] sent SYNC to %s (%d bytes)", clientIP, syncN)
				}
			}
		} else {
			logging.Logf("[error] registration failed from %s: no valid services", clientIP)
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

	// Keep reading subsequent REGISTER, SYNC, FORWARD, or DATA lines.
	// Client sends periodic heartbeat (default 10s); no separate heartbeat protocol needed.
	// DATA lines can appear on control connection when peer sends data after FORWARD command.
	for {
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		line, err := reader.ReadString('\n')
		conn.SetReadDeadline(time.Time{})
		if err != nil {
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[debug] control connection closed from %s: %v", clientIP, err)
			}
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Check if it's a DATA line (data stream on control connection)
		// This happens when peer sends data after FORWARD command on the same control connection
		if proxyID, ok := protocol.ParseDataLine(line); ok {
			logging.Logf("[data] received DATA header from %s proxy_id=%s", clientIP, proxyID)
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[debug] DATA line on control connection from %s proxy_id=%s", clientIP, proxyID)
			}
			// Find the pending DATA channel for this proxy_id
			s.pendingLock.Lock()
			ch := s.pendingData[proxyID]
			if ch != nil {
				delete(s.pendingData, proxyID)
			}
			s.pendingLock.Unlock()

			if ch != nil {
				// Peer-to-peer data transmission: reuse control connection (long connection)
				// Extract any buffered data from bufio.Reader before switching to stream mode
				logging.Logf("[data] matched proxy_id=%s from %s, switching to stream mode", proxyID, clientIP)
				if cfg != nil && cfg.Log.Level == "debug" {
					logging.Logf("[debug] DATA matched, switching to stream mode proxy_id=%s", proxyID)
				}
				// Check if bufio.Reader has buffered data that needs to be passed along
				// The reader may have already read some data beyond the DATA header line
				bufferedData := []byte(nil)
				if reader.Buffered() > 0 {
					bufferedData = make([]byte, reader.Buffered())
					n, _ := reader.Read(bufferedData)
					bufferedData = bufferedData[:n]
					if cfg != nil && cfg.Log.Level == "debug" {
						logging.Logf("[debug] extracted %d buffered bytes for proxy_id=%s", len(bufferedData), proxyID)
					}
				}
				// Create a buffered connection wrapper that includes any buffered data
				// This ensures the waiting goroutine can read all data, including what was buffered
				var dataConn net.Conn = conn
				if len(bufferedData) > 0 {
					dataConn = &proxy.BufferedConn{Conn: conn, Buf: bufferedData, Pos: 0}
					if cfg != nil && cfg.Log.Level == "debug" {
						logging.Logf("[debug] created BufferedConn with %d bytes for proxy_id=%s", len(bufferedData), proxyID)
					}
				}
				// Send the connection (with buffered data) to the waiting goroutine
				// The goroutine will read data stream directly from the connection
				ch <- dataConn
				// IMPORTANT: After sending DATA connection, we need to pause command reading
				// because the data stream will be read by the goroutine. We cannot continue
				// reading commands while data is being transmitted.
				// The control connection reading loop will continue after data transmission completes.
				// However, we cannot block here, so we continue the loop and let the goroutine
				// handle data reading. The next ReadString will block until data transmission
				// completes or times out, which is acceptable.
				continue
			} else {
				logging.Logf("[error] unexpected DATA from %s proxy_id=%s: no pending forward", clientIP, proxyID)
				if cfg != nil && cfg.Log.Level == "debug" {
					// Log all pending proxy IDs for debugging
					s.pendingLock.Lock()
					pendingIDs := make([]string, 0, len(s.pendingData))
					for id := range s.pendingData {
						pendingIDs = append(pendingIDs, id)
					}
					s.pendingLock.Unlock()
					logging.Logf("[debug] current pending proxy_ids: %v", pendingIDs)
				}
				continue
			}
		}

		// Check if it's a FORWARD command (request to forward traffic from another peer)
		// When Peer B sends FORWARD to Peer A, Peer A receives it here on the control connection
		// Peer A should then connect to local backend and establish DATA connection back to Peer B
		if strings.HasPrefix(line, protocol.CmdForward+":") {
			logging.Logf("[forward] received FORWARD command from %s", clientIP)
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[debug] FORWARD line=%q", strings.TrimSpace(line))
			}
			proxyID, name, backendAddr, ok := protocol.ParseForwardLine(line)
			if ok {
				logging.Logf("[forward] parsed proxy_id=%s name=%s backend=%s from %s", proxyID, name, backendAddr, clientIP)
				if cfg != nil && cfg.Log.Level == "debug" {
					logging.Logf("[debug] FORWARD parsed proxy_id=%s name=%s backend=%s", proxyID, name, backendAddr)
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
						if cfg != nil && cfg.Log.Level == "debug" {
							logging.Logf("[debug] found peer from peerServices peer_ip=%s peer_id=%s peer_addr=%s", peerIP, peerSvc.PeerID, peerSvc.PeerAddr)
						}
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
								if cfg != nil && cfg.Log.Level == "debug" {
									logging.Logf("[debug] found peer from services key=%s peer_ip=%s peer_id=%s", key, connRemoteIP, peerSvc.PeerID)
								}
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
					if cfg != nil && cfg.Log.Level == "debug" {
						logging.Logf("[debug] using peer_addr=%s from peerServices peer_id=%s", sourcePeerAddr, peerSvcInfo.PeerID)
					}
				} else if peerConn != nil && peerConn.RemoteAddr() != nil {
					// Fallback: use control connection's remote address
					// This assumes the peer's bind address is the same as control connection address
					sourcePeerAddr = peerConn.RemoteAddr().String()
					logging.Logf("[error] using control connection address as peer_addr=%s (peer may not have sent SYNC with PeerAddr)", sourcePeerAddr)
				}

				if sourcePeerAddr == "" {
					logging.Logf("[error] cannot determine peer address for FORWARD proxy_id=%s name=%s", proxyID, name)
					if s.collector != nil {
						s.collector.RecordProxyError(name, "data_dial_error")
					}
					continue // Skip this FORWARD request
				}

				// Log whether service is local or remote
				serviceType := "remote"
				if serviceClient != nil && serviceClient.IP == "local" {
					serviceType = "local"
					if cfg != nil && cfg.Log.Level == "debug" {
						logging.Logf("[debug] FORWARD for local service name=%s backend=%s", name, backendAddr)
					}
				}

				logging.Logf("[forward] handling proxy_id=%s name=%s backend=%s from=%s type=%s", proxyID, name, backendAddr, sourcePeerAddr, serviceType)
				if cfg != nil && cfg.Log.Level == "debug" {
					logging.Logf("[debug] FORWARD details proxy_id=%s name=%s backend=%s from=%s type=%s", proxyID, name, backendAddr, sourcePeerAddr, serviceType)
				}

				// Validate sourcePeerAddr before calling handleForwardRequest
				if sourcePeerAddr == "" {
					logging.Logf("[error] peer address is empty, cannot handle FORWARD proxy_id=%s name=%s", proxyID, name)
					if s.collector != nil {
						s.collector.RecordProxyError(name, "data_dial_error")
					}
					continue // Skip this FORWARD request
				}

				// Handle FORWARD request: connect to backend and establish DATA connection
				// Run in goroutine to avoid blocking the control connection
				// Pass the control connection so we can use it to find the peer's connection info
				if cfg != nil && cfg.Log.Level == "debug" {
					logging.Logf("[debug] starting FORWARD handler proxy_id=%s name=%s peer_addr=%s", proxyID, name, sourcePeerAddr)
				}
				go func() {
					defer func() {
						if r := recover(); r != nil {
							logging.Logf("[error] panic in handleForwardRequest proxy_id=%s name=%s err=%v", proxyID, name, r)
						}
					}()
					s.handleForwardRequestWithConn(conn, sourcePeerAddr, proxyID, name, backendAddr, cfg)
				}()
			} else {
				logging.Logf("[error] invalid FORWARD message from %s: %q", clientIP, line)
			}
		} else if strings.HasPrefix(line, protocol.CmdSync+":") {
			// Check if it's a SYNC command (service list from peer)
			regs, ok := protocol.ParseSyncLine(line)
			if ok {
				// Pass the connection that sent the SYNC message
				s.processSync(clientIP, regs, conn, cfg)
			} else {
				logging.Logf("[error] invalid SYNC message from %s: %q", clientIP, line)
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
			logging.Logf("[sync] warning: peer_id is empty from %s, using IP as fallback", peerIP)
		}
	} else {
		logging.Logf("[sync] warning: no registrations in SYNC from %s, cannot extract peer_id", peerIP)
	}

	logging.Logf("[sync] processing SYNC from %s peer_id=%s count=%d", peerIP, syncPeerID, len(regs))
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] SYNC conn=%v conn_info=%s", conn != nil, connInfo)
	}

	// Use the connection that sent the SYNC message
	// This is the control connection from the peer
	peerConn := conn

	// Also try to find any active connection from this peer IP
	// This handles cases where the connection might have been registered with a different IP
	// (e.g., when connecting through a load balancer)
	if peerConn == nil {
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] SYNC conn is nil, searching for peer connection peer_id=%s peer_ip=%s", syncPeerID, peerIP)
		}
		for key, svc := range s.services {
			if svc != nil && svc.IP == peerIP {
				if cfg != nil && cfg.Log.Level == "debug" {
					logging.Logf("[debug] found service from peer key=%s ip=%s conn=%v connected=%t", key, svc.IP, svc.Conn != nil, svc.Connected)
				}
				if svc.Conn != nil && svc.Connected {
					peerConn = svc.Conn
					if cfg != nil && cfg.Log.Level == "debug" {
						logging.Logf("[debug] using existing connection for peer_id=%s peer_ip=%s", syncPeerID, peerIP)
					}
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
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[debug] trying to find connection by remote IP peerIP=%s connRemoteIP=%s", peerIP, connRemoteIP)
			}
			for key, svc := range s.services {
				if svc != nil && svc.Conn == conn {
					if cfg != nil && cfg.Log.Level == "debug" {
						logging.Logf("[debug] found connection match key=%s ip=%s", key, svc.IP)
					}
					peerConn = conn
					break
				}
			}
		}
	}

	if peerConn == nil {
		logging.Logf("[error] no connection available for SYNC from %s peer_id=%s, services will be registered but cannot be forwarded", peerIP, syncPeerID)
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] conn parameter was %v, peerIP=%s", conn != nil, peerIP)
		}
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
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[debug] peer connection changed peer_id=%s peer_ip=%s", syncPeerID, peerIP)
			}
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
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[debug] storing peerServices by conn RemoteAddr IP=%s (peerIP=%s)", connRemoteIP, peerIP)
			}
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
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] registering synced service name=%q backend=%q peer_id=%q peer_ip=%s peer_addr=%q",
				name, backend, syncPeerID, peerIP, syncPeerAddr)
		}
		s.RegisterClientByNameWithPeerInfo(name, peerIP, backend, peerConn, syncPeerID, syncPeerAddr)
	}

	logging.Logf("[sync] updated peer services peer_id=%s peer_ip=%s count=%d", syncPeerID, peerIP, len(peerSvc.Services))

	// Step 3: Print full peerServices map structure
	// Design doc requirement: print complete peerServices map structure
	s.logPeerServicesMap()
}

// handleForwardRequestWithConn handles a FORWARD request using the existing control connection
// The control connection (conn) is the established duplex connection between peers
// We use this connection to find the peer and establish DATA connection
func (s *ProxyServer) handleForwardRequestWithConn(controlConn net.Conn, peerAddr, proxyID, name, backendAddr string, cfg *config.Config) {
	logging.Logf("[forward] handling proxy_id=%s name=%s backend=%s from=%s", proxyID, name, backendAddr, peerAddr)
	if cfg != nil && cfg.Log.Level == "debug" {
		controlRemote := ""
		if controlConn != nil && controlConn.RemoteAddr() != nil {
			controlRemote = controlConn.RemoteAddr().String()
		}
		logging.Logf("[debug] FORWARD request control_remote=%s", controlRemote)
	}

	dialTimeout := 5 * time.Second
	if cfg != nil {
		dialTimeout = cfg.GetDialTimeout()
	}
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] FORWARD handler started proxy_id=%s dial_timeout=%s", proxyID, dialTimeout)
	}

	// Step 1: Connect to local backend
	logging.Logf("[forward] connecting to backend proxy_id=%s backend=%s", proxyID, backendAddr)
	startBackendDial := time.Now()
	backendConn, err := net.DialTimeout("tcp", backendAddr, dialTimeout)
	backendDialDuration := time.Since(startBackendDial)
	if err != nil {
		logging.Logf("[error] backend dial failed proxy_id=%s backend=%s err=%v duration=%s", proxyID, backendAddr, err, backendDialDuration)
		if s.collector != nil {
			s.collector.RecordProxyError(name, "backend_dial_error")
		}
		return
	}
	defer backendConn.Close()
	logging.Logf("[forward] backend connected proxy_id=%s backend=%s duration=%s", proxyID, backendAddr, backendDialDuration)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] backend connected local=%s", backendConn.LocalAddr())
	}

	// Step 2: Use the existing control connection to send data (peer-to-peer: long connection)
	// For peer-to-peer communication, reuse the control connection instead of creating a new DATA connection
	// This is more efficient and reduces connection overhead between peers
	if controlConn == nil {
		logging.Logf("[error] control connection is nil, cannot send data proxy_id=%s", proxyID)
		if s.collector != nil {
			s.collector.RecordProxyError(name, "data_dial_error")
		}
		return
	}

	// Send DATA header to identify this data stream on the control connection
	dataHeader := protocol.FormatData(proxyID)
	controlConnRemote := ""
	if controlConn.RemoteAddr() != nil {
		controlConnRemote = controlConn.RemoteAddr().String()
	}
	logging.Logf("[data] sending DATA header proxy_id=%s name=%s to=%s", proxyID, name, controlConnRemote)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] DATA header=%q", strings.TrimSpace(dataHeader))
	}

	// Send DATA header on control connection to mark the start of data transmission
	if _, err := controlConn.Write([]byte(dataHeader)); err != nil {
		logging.Logf("[error] DATA header send failed proxy_id=%s err=%v", proxyID, err)
		if s.collector != nil {
			s.collector.RecordProxyError(name, "data_header_error")
		}
		return
	}
	logging.Logf("[data] DATA header sent proxy_id=%s", proxyID)

	// Use control connection as data connection
	dataConn := controlConn

	logging.Logf("[forward] bridge started proxy_id=%s name=%s backend=%s", proxyID, name, backendAddr)

	// Step 4: Bridge data in both directions
	var bytesToPeer, bytesToBackend int64
	errCh := make(chan error, 2)

	go func() {
		n, e := io.Copy(dataConn, backendConn) // backend -> peer
		bytesToPeer = n
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] backend->peer done proxy_id=%s bytes=%d err=%v", proxyID, n, e)
		}
		// Don't close control connection write side - it's still used for other commands
		// Only signal EOF by not writing more data
		errCh <- e
	}()

	go func() {
		n, e := io.Copy(backendConn, dataConn) // peer -> backend
		bytesToBackend = n
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] peer->backend done proxy_id=%s bytes=%d err=%v", proxyID, n, e)
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

	logging.Logf("[forward] bridge completed proxy_id=%s name=%s bytes_to_peer=%d bytes_to_backend=%d", proxyID, name, bytesToPeer, bytesToBackend)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] bridge errors err1=%v err2=%v", err1, err2)
	}

	// Return first non-nil error
	if err1 != nil && err1 != io.EOF {
		logging.Logf("[error] forward failed proxy_id=%s name=%s err=%v", proxyID, name, err1)
		if s.collector != nil {
			s.collector.RecordProxyError(name, "forward_io_error")
		}
		return
	}
	if err2 != nil && err2 != io.EOF {
		logging.Logf("[error] forward failed proxy_id=%s name=%s err=%v", proxyID, name, err2)
		if s.collector != nil {
			s.collector.RecordProxyError(name, "forward_io_error")
		}
		return
	}

	logging.Logf("[forward] completed proxy_id=%s name=%s", proxyID, name)
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
				// First try: find from peerServices map by IP (most reliable)
				// peerServices map now stores PeerAddr directly, so we can use it
				if peerSvc, exists := s.peerServices[remoteIP]; exists && peerSvc != nil {
					if peerSvc.PeerAddr != "" {
						return peerSvc.PeerAddr
					}
				}

				// Second try: find any service from this peer IP (not local services, as they don't have PeerAddr)
				// This helps when the requested service is local (ip=local) but we need the peer's bind address
				// The peer that sent FORWARD should have registered other services via SYNC, which have PeerAddr
				for _, client := range s.services {
					if client == nil {
						continue
					}
					// Check if IP matches (for remote services, IP is the peer's IP)
					// Exclude local services as they don't have PeerAddr
					if client.IP == remoteIP && client.IP != "local" && client.PeerAddr != "" {
						return client.PeerAddr
					}
				}
			}
		}
	}

	logging.Logf("[server] ERROR: peer_addr not found for conn=%v, will use RemoteAddr as fallback (this will likely fail)", connRemote)
	return ""
}
