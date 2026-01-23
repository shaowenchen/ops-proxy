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
		// Extract peer_id and peer_addr from first registration (all should have the same)
		var peerID, peerAddr string
		if len(regs) > 0 {
			peerID = regs[0].PeerID
			peerAddr = regs[0].PeerAddr
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

		// Check if it's a FORWARD command (request to forward traffic from another peer)
		// When Peer B sends FORWARD to Peer A, Peer A receives it here on the control connection
		// Peer A should then connect to local backend and establish DATA connection back to Peer B
		if strings.HasPrefix(line, protocol.CmdForward+":") {
			proxyID, name, backendAddr, ok := protocol.ParseForwardLine(line)
			if ok {
				// Find the peer that sent this FORWARD command using the connection
				// We need the peer's real bind address (not the central gateway address)
				sourcePeerAddr := s.findPeerAddrByConn(conn)
				if sourcePeerAddr == "" {
					// Fallback to connection's remote address (might be gateway)
					if conn != nil && conn.RemoteAddr() != nil {
						sourcePeerAddr = conn.RemoteAddr().String()
					}
				}
				logging.Logf("[server] RECEIVED FORWARD proxy_id=%s name=%s backend=%s from_peer=%s", proxyID, name, backendAddr, sourcePeerAddr)
				if cfg != nil && cfg.Log.Level == "debug" {
					logging.Logf("[request][debug] FORWARD command received (from_peer=%s proxy_id=%s name=%s backend=%s)", sourcePeerAddr, proxyID, name, backendAddr)
				}
				// Handle FORWARD request: connect to backend and establish DATA connection
				// Run in goroutine to avoid blocking the control connection
				go s.handleForwardRequest(sourcePeerAddr, proxyID, name, backendAddr, cfg)
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
	// Extract peer_id and peer_addr from first registration (all should have the same)
	var syncPeerID, syncPeerAddr string
	if len(regs) > 0 {
		syncPeerID = regs[0].PeerID
		syncPeerAddr = regs[0].PeerAddr
	}
	for _, reg := range regs {
		name := strings.TrimSpace(reg.Name)
		backend := strings.TrimSpace(reg.Backend)
		if name == "" || backend == "" {
			continue
		}
		logging.Logf("[registry] registering synced service name=%q backend=%q peer=%s peer_id=%q peer_addr=%q conn=%v", name, backend, peerIP, syncPeerID, syncPeerAddr, peerConn != nil)
		s.RegisterClientByNameWithPeerInfo(name, peerIP, backend, peerConn, syncPeerID, syncPeerAddr)
	}

	logging.Logf("[registry] updated peer services (peer=%s count=%d)", peerIP, len(peerSvc.Services))

	// Step 3: Print full peerServices map structure
	// Design doc requirement: print complete peerServices map structure
	s.logPeerServicesMap()
}

// handleForwardRequest handles a FORWARD request from another peer
// This is called when Peer A receives FORWARD command from Peer B
// Design doc flow:
// 1. Peer B sends: FORWARD:proxy_id:name:backend
// 2. Peer A connects to local backend
// 3. Peer A establishes DATA connection back to Peer B
// 4. Bridge data between client request and backend response
func (s *ProxyServer) handleForwardRequest(peerAddr, proxyID, name, backendAddr string, cfg *config.Config) {
	logging.Logf("[server] handling FORWARD proxy_id=%s name=%s backend=%s from_peer=%s", proxyID, name, backendAddr, peerAddr)

	dialTimeout := 5 * time.Second
	if cfg != nil {
		dialTimeout = cfg.GetDialTimeout()
	}

	// Step 1: Connect to local backend
	logging.Logf("[server] connecting to backend proxy_id=%s backend=%s", proxyID, backendAddr)
	backendConn, err := net.DialTimeout("tcp", backendAddr, dialTimeout)
	if err != nil {
		logging.Logf("[server] backend dial failed proxy_id=%s backend=%s err=%v", proxyID, backendAddr, err)
		if s.collector != nil {
			s.collector.RecordProxyError(name, "backend_dial_error")
		}
		return
	}
	defer backendConn.Close()
	logging.Logf("[server] backend connected proxy_id=%s backend=%s local=%s", proxyID, backendAddr, backendConn.LocalAddr())

	// Step 2: Connect back to requesting peer for DATA channel
	logging.Logf("[server] connecting DATA channel proxy_id=%s peer=%s", proxyID, peerAddr)
	dataConn, err := net.DialTimeout("tcp", peerAddr, dialTimeout)
	if err != nil {
		logging.Logf("[server] DATA dial failed proxy_id=%s peer=%s err=%v", proxyID, peerAddr, err)
		if s.collector != nil {
			s.collector.RecordProxyError(name, "data_dial_error")
		}
		return
	}
	defer dataConn.Close()

	// Step 3: Send DATA header to identify this connection
	if _, err := dataConn.Write([]byte(protocol.FormatData(proxyID))); err != nil {
		logging.Logf("[server] DATA header send failed proxy_id=%s err=%v", proxyID, err)
		if s.collector != nil {
			s.collector.RecordProxyError(name, "data_header_error")
		}
		return
	}
	logging.Logf("[server] DATA channel established proxy_id=%s", proxyID)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[request][debug] DATA channel connected (proxy_id=%s local=%s remote=%s)", proxyID, dataConn.LocalAddr(), dataConn.RemoteAddr())
	}

	logging.Logf("[server] bridge start proxy_id=%s name=%s backend=%s", proxyID, name, backendAddr)

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
		logging.Logf("[server] using LOCAL_PEER_ADDR as peer_addr: %s", cfg.Peer.LocalPeerAddr)
		return cfg.Peer.LocalPeerAddr
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
	
	s.clientsLock.RLock()
	defer s.clientsLock.RUnlock()
	
	connRemote := "unknown"
	if conn.RemoteAddr() != nil {
		connRemote = conn.RemoteAddr().String()
	}
	
	logging.Logf("[server] findPeerAddrByConn: searching for conn=%v, total_services=%d", connRemote, len(s.services))
	
	// Find any service registered with this connection
	for key, client := range s.services {
		if client == nil {
			continue
		}
		
		// Check if this is the same connection
		if client.Conn == conn {
			logging.Logf("[server] findPeerAddrByConn: found match key=%s peer_id=%s peer_addr=%s ip=%s", key, client.PeerID, client.PeerAddr, client.IP)
			if client.PeerAddr != "" {
				logging.Logf("[server] found peer_addr=%s (peer_id=%s) for conn=%v", client.PeerAddr, client.PeerID, connRemote)
				return client.PeerAddr
			} else {
				logging.Logf("[server] peer_addr is empty for key=%s (peer_id=%s), checking other services with same conn", key, client.PeerID)
			}
		}
	}
	
	logging.Logf("[server] peer_addr not found for conn=%v, will use RemoteAddr as fallback", connRemote)
	return ""
}
