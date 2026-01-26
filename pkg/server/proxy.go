package server

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/ops-proxy/pkg/config"
	"github.com/ops-proxy/pkg/logging"
	wire "github.com/ops-proxy/pkg/protocol"
	"github.com/ops-proxy/pkg/proxy"
	"github.com/ops-proxy/pkg/types"
)

// StartProxyListener starts the proxy listener
func (s *ProxyServer) StartProxyListener(bindAddr string, cfg *config.Config) error {
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", bindAddr, err)
	}

	logging.Logf("[listen] proxy addr=%s", bindAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			logging.Logf("Error accepting connection: %v", err)
			continue
		}

		// Unified port:
		// - REGISTER:/DATA: => client control/data (registration handler)
		// - otherwise => proxy traffic (HTTP/HTTPS/TCP)
		go s.handleUnifiedConnection(conn, cfg)
	}
}

func (s *ProxyServer) handleUnifiedConnection(conn net.Conn, cfg *config.Config) {
	remote := ""
	if conn != nil && conn.RemoteAddr() != nil {
		remote = conn.RemoteAddr().String()
	}

	// Log every incoming connection at debug level
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] new connection from %s", remote)
	}

	// Read initial data to classify connection.
	buf := make([]byte, 4096)
	readTimeout := 5 * time.Second
	if cfg != nil {
		readTimeout = cfg.GetReadTimeout()
	}
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
	n, err := conn.Read(buf)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		if err == io.EOF {
			s.logAcceptEOF(remote, cfg)
		} else {
			logging.Logf("[accept] Error reading connection (remote=%s): %v", remote, err)
		}
		_ = conn.Close()
		return
	}

	// Strip PROXY protocol (if present) so we can correctly detect REGISTER/DATA and Host/SNI.
	payload, ppInfo := stripProxyProtocol(conn, nil, buf[:n], readTimeout)
	if ppInfo != "" && cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[accept][debug] %s remote=%s", ppInfo, remote)
	}
	// If PROXY header was incomplete, try to read more and strip again.
	if strings.Contains(ppInfo, "incomplete=true") {
		more := make([]byte, 4096)
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		m, readErr := conn.Read(more)
		_ = conn.SetReadDeadline(time.Time{})
		if readErr != nil {
			logging.Logf("[accept] Error reading more PROXY header bytes (remote=%s): %v", remote, readErr)
			_ = conn.Close()
			return
		}
		combined := append(payload, more[:m]...)
		payload, ppInfo = stripProxyProtocol(conn, nil, combined, readTimeout)
		if ppInfo != "" && cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[accept][debug] %s remote=%s", ppInfo, remote)
		}
	}
	// If PROXY header consumed all initial bytes, read again to get actual payload.
	if len(payload) == 0 {
		more := make([]byte, 4096)
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		m, readErr := conn.Read(more)
		_ = conn.SetReadDeadline(time.Time{})
		if readErr != nil {
			logging.Logf("[accept] Error reading payload after PROXY header (remote=%s): %v", remote, readErr)
			_ = conn.Close()
			return
		}
		payload = more[:m]
	}

	// Detect registration/control/data by prefix.
	line := bytes.TrimSpace(payload)
	if bytes.HasPrefix(line, []byte(wire.CmdRegister+":")) || bytes.HasPrefix(line, []byte(wire.CmdData+":")) {
		// Log every registration/data request
		prefixLen := len(line)
		if prefixLen > 20 {
			prefixLen = 20
		}
		logging.Logf("[accept] registration/data request (remote=%s bytes=%d prefix=%q)", remote, len(payload), string(line[:prefixLen]))
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] registration/data request (remote=%s bytes=%d)", remote, len(payload))
		}
		bufConn := &proxy.BufferedConn{Conn: conn, Buf: payload, Pos: 0}
		s.HandleClientRegistration(bufConn, cfg)
		return
	}

	// Otherwise, treat it as proxy traffic.
	defer conn.Close()
	// Log is already in handleProxyConnectionFromInitial
	s.handleProxyConnectionFromInitial(conn, cfg, payload, len(payload), remote, readTimeout)
}

// handleProxyConnection handles proxy connection using domain-based routing
// Extracts domain from Host header (HTTP) or SNI (HTTPS) to route to the correct client
func (s *ProxyServer) handleProxyConnection(conn net.Conn, cfg *config.Config) {
	defer conn.Close()
	remote := ""
	if conn != nil && conn.RemoteAddr() != nil {
		remote = conn.RemoteAddr().String()
	}
	// Read initial data to detect protocol and route
	buf := make([]byte, 4096)
	readTimeout := 5 * time.Second
	if cfg != nil {
		readTimeout = cfg.GetReadTimeout()
	}
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
	n, err := conn.Read(buf)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		// EOF is common for health checks / short-lived connects; keep it debug-only to reduce noise.
		if err == io.EOF {
			s.logAcceptEOF(remote, cfg)
		} else {
			logging.Logf("[accept] Error reading connection (remote=%s): %v", remote, err)
		}
		return
	}
	s.handleProxyConnectionFromInitial(conn, cfg, buf[:n], n, remote, readTimeout)
}

func (s *ProxyServer) handleProxyConnectionFromInitial(conn net.Conn, cfg *config.Config, initial []byte, n int, remote string, readTimeout time.Duration) {
	// Log every proxy request at debug level
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] proxy request from %s bytes=%d", remote, n)
	}

	// Detect protocol type and route
	// No default client - if no service found, return error
	getDefaultClient := func() string {
		return "" // No default client
	}
	extractHost := func(data []byte) string {
		host := proxy.ExtractHostFromHTTP(data)
		if cfg != nil && cfg.Log.Level == "debug" && host != "" {
			logging.Logf("[debug] extracted HTTP Host=%q from %s", host, remote)
		}
		return host
	}
	extractSNI := func(data []byte) string {
		sni := proxy.ExtractSNI(data)
		if cfg != nil && cfg.Log.Level == "debug" && sni != "" {
			logging.Logf("[debug] extracted TLS SNI=%q from %s", sni, remote)
		}
		return sni
	}
	protocol, clientName := proxy.DetectProtocolAndRoute(initial, getDefaultClient, extractHost, extractSNI)
	extractedName := clientName

	// Log protocol and extracted name immediately after detection
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] protocol=%s name=%q from %s", protocol, extractedName, remote)
	}

	// Forward proxy (HTTP CONNECT).
	// HTTP CONNECT directly forwards to target address (no service routing needed)
	// But if ALL_PROXY is set, route through the specified peer ID
	if protocol == "http_connect" {
		host, port, ok := proxy.ExtractConnectHostPort(initial)
		if !ok {
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[debug] HTTP CONNECT parse failed (remote=%s)", remote)
			}
			logging.Logf("HTTP CONNECT detected but failed to parse target")
			return
		}
		targetAddr := net.JoinHostPort(host, port)

		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] HTTP CONNECT request (remote=%s target=%s)", remote, targetAddr)
		}

		// Check if ALL_PROXY is set with peer ID
		allProxyPeerID := parseAllProxyPeerID()
		if allProxyPeerID != "" {
			proxyClient := s.GetClientByPeerID(allProxyPeerID)
			if proxyClient != nil && proxyClient.Connected && proxyClient.Conn != nil {
				// Route through the specified peer ID
				logging.Logf("[route] HTTP CONNECT routing through ALL_PROXY peer_id=%s target=%s", allProxyPeerID, targetAddr)
				// Create a virtual service entry for routing
				virtualClient := &types.ClientInfo{
					Name:        targetAddr, // Use target address as service name
					IP:          proxyClient.IP,
					BackendAddr: targetAddr,
					PeerID:      allProxyPeerID,
					PeerAddr:    proxyClient.PeerAddr,
					Conn:        proxyClient.Conn,
					Connected:   true,
					LastSeen:    time.Now(),
				}
				// Forward through the peer
				end := proxy.FindHTTPEndOfHeaders(initial)
				pos := n
				if end >= 0 {
					pos = end + 4
					if pos > n {
						pos = n
					}
				}
				bufConn := &proxy.BufferedConn{
					Conn: conn,
					Buf:  initial,
					Pos:  pos,
				}
				updateMetrics := func(clientName, protocol string, success bool, bytesTx, bytesRx int64, duration time.Duration) {
					if s.collector != nil {
						s.collector.UpdateConnectionMetrics(clientName, protocol, success, bytesTx, bytesRx, duration)
					}
				}
				s.forwardOnce(bufConn, conn, targetAddr, "http_connect", virtualClient, cfg, updateMetrics, targetAddr)
				return
			} else {
				logging.Logf("[route] ALL_PROXY peer_id=%s not found or not connected for HTTP CONNECT, using direct forward", allProxyPeerID)
			}
		}

		end := proxy.FindHTTPEndOfHeaders(initial)
		pos := n
		if end >= 0 {
			pos = end + 4
			if pos > n {
				pos = n
			}
		}
		bufConn := &proxy.BufferedConn{
			Conn: conn,
			Buf:  initial,
			Pos:  pos,
		}

		// Reply 200 to the proxy client, then start tunneling.
		_, _ = conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))

		updateMetrics := func(clientName, protocol string, success bool, bytesTx, bytesRx int64, duration time.Duration) {
			if s.collector != nil {
				s.collector.UpdateConnectionMetrics(clientName, protocol, success, bytesTx, bytesRx, duration)
			}
		}

		// HTTP CONNECT directly forwards to target (no service routing)
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] CONNECT direct dial (remote=%s target=%s)", remote, targetAddr)
		}
		s.forwardDirect(bufConn, conn, host, "forward", targetAddr, cfg, updateMetrics)
		return
	}

	// Log protocol detection result - always log at debug level
	if cfg != nil && cfg.Log.Level == "debug" {
		if clientName == "" {
			maxLen := 100
			if n < maxLen {
				maxLen = n
			}
			if protocol == "http" {
				logging.Logf("[debug] protocol detection protocol=http host_found=false bytes=%d from %s", n, remote)
			} else if protocol == "https" {
				maxLen = 20
				if n < maxLen {
					maxLen = n
				}
				logging.Logf("[debug] protocol detection protocol=https sni_found=false bytes=%d from %s", n, remote)
			} else {
				maxLen = 20
				if n < maxLen {
					maxLen = n
				}
				logging.Logf("[debug] protocol detection protocol=tcp bytes=%d preview=%x from %s", n, initial[:maxLen], remote)
			}
		}
	}

	// If clientName is empty (e.g., Host header not found or SNI extraction failed), return error
	if clientName == "" {
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] route selection failed protocol=%s reason=no_host_sni from %s", protocol, remote)
		}
		logging.Logf("[route] select_failed protocol=%s reason=no_host_sni", protocol)
		// Return error response for HTTP requests
		if protocol == "http" {
			errorMsg := "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 40\r\n\r\nBad Request: No Host header or SNI found\n"
			_, _ = conn.Write([]byte(errorMsg))
		} else if protocol == "https" {
			// For HTTPS, close connection (can't send HTTP error)
			_ = conn.Close()
		} else {
			// For raw TCP, close connection
			_ = conn.Close()
		}
		return
	}

	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] route selected name=%q protocol=%s from %s", clientName, protocol, remote)
	}
	logging.Logf("[route] selected name=%q protocol=%s reason=extracted", clientName, protocol)

	// Debug: show selected service candidates for this name.
	// Always show this to help diagnose routing issues
	// Force flush log immediately to ensure visibility
	logging.Logf("[route] STEP1: checking candidates for name=%q", clientName)
	logging.Logf("[route] STEP2: about to call servicesByNameSnapshot name=%q", clientName)
	var candidates string
	func() {
		defer func() {
			if r := recover(); r != nil {
				logging.Logf("[route] panic in servicesByNameSnapshot: %v", r)
				candidates = "<panic>"
			}
		}()
		logging.Logf("[route] STEP3: calling servicesByNameSnapshot name=%q", clientName)
		candidates = s.servicesByNameSnapshot(clientName)
		logging.Logf("[route] STEP4: servicesByNameSnapshot returned candidates=%q", candidates)
	}()
	logging.Logf("[route] STEP5: after servicesByNameSnapshot, candidates=%q", candidates)
	logging.Logf("[route] candidates name=%q items=%s", clientName, candidates)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] svc candidates (name=%q items=%s)", clientName, candidates)
	}

	// Debug: show extracted domain, selected client, and current service pool on this server instance.
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] route details protocol=%s extracted=%q selected=%q from %s", protocol, extractedName, clientName, remote)
		// Don't print full service table on every route - too verbose
		// Service table is printed on registration/sync events
	}

	// Get client and immediately log service type (local or remote) for visibility
	var client *types.ClientInfo

	// Check if ALL_PROXY is set with peer ID format: socks5://peerid:port
	// If set, route all requests through that peer ID
	allProxyPeerID := parseAllProxyPeerID()
	if allProxyPeerID != "" {
		logging.Logf("[route] ALL_PROXY peer_id=%s detected, routing through peer", allProxyPeerID)
		proxyClient := s.GetClientByPeerID(allProxyPeerID)
		if proxyClient != nil && proxyClient.Connected && proxyClient.Conn != nil {
			// Use the peer ID's connection to forward the request
			// Create a virtual service entry for routing
			client = &types.ClientInfo{
				Name:        clientName,
				IP:          proxyClient.IP,
				BackendAddr: clientName, // Use clientName as backend (will be forwarded to peer)
				PeerID:      allProxyPeerID,
				PeerAddr:    proxyClient.PeerAddr,
				Conn:        proxyClient.Conn,
				Connected:   true,
				LastSeen:    time.Now(),
			}
			logging.Logf("[route] using ALL_PROXY peer_id=%s for routing name=%s", allProxyPeerID, clientName)
		} else {
			logging.Logf("[route] ALL_PROXY peer_id=%s not found or not connected, falling back to normal routing", allProxyPeerID)
		}
	}

	// If not using ALL_PROXY or peer not found, use normal routing
	if client == nil {
		logging.Logf("[route] calling GetClient name=%q", clientName)
		func() {
			defer func() {
				if r := recover(); r != nil {
					logging.Logf("[route] panic in GetClient: %v", r)
					client = nil
				}
			}()
			client = s.GetClient(clientName)
		}()
		logging.Logf("[route] GetClient returned client=%v", client != nil)
	}
	if client != nil {
		serviceType := "remote"
		if client.IP == "local" {
			serviceType = "local"
		}
		logging.Logf("[route] service type=%s name=%s ip=%s backend=%s connected=%t conn=%v", serviceType, clientName, client.IP, client.BackendAddr, client.Connected, client.Conn != nil)
	} else {
		// Client not found - log this immediately for debugging
		logging.Logf("[route] service not found name=%s (remote=%s protocol=%s)", clientName, remote, protocol)
		logging.Logf("[tunnel] Client %s not found (remote=%s protocol=%s)", clientName, remote, protocol)
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] client not found (remote=%s name=%s protocol=%s)", remote, clientName, protocol)
		}
		if s.collector != nil {
			s.collector.RecordProxyError(clientName, "no_client")
		}
		// No fallback - if service not found, return error
		if client == nil {
			logging.Logf("[tunnel] No client available for proxy")
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[debug] no client available (remote=%s protocol=%s)", remote, protocol)
			}
			if s.collector != nil {
				s.collector.RecordProxyError(clientName, "no_client")
			}
			// Return error response for HTTP requests
			if protocol == "http" || protocol == "http_connect" {
				errorMsg := fmt.Sprintf("HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\nService not available: %s\n", len("Service not available: ")+len(clientName)+1, clientName)
				_, _ = conn.Write([]byte(errorMsg))
			}
			return
		}
	}

	// Log client details - this shows whether it's local or remote
	serviceType := "remote"
	if client.IP == "local" {
		serviceType = "local"
	}
	logging.Logf("[tunnel] client found name=%s type=%s remote_peer_addr=%s connected=%t backend=%s", clientName, serviceType, client.IP, client.Connected, client.BackendAddr)
	logging.Logf("[route] SELECTED service=%s type=%s backend=%s peer_id=%s", clientName, serviceType, client.BackendAddr, client.PeerID)

	// Determine forwarding method based on service location (local vs remote peer)
	// According to design document:
	// - If service is local (IP="local"): forward directly to local backend
	// - If service is remote: generate proxy_id and send FORWARD command to peer
	if client.IP == "local" {
		// Local service - direct forward to local backend
		// Design doc flow: Peer A receives request for test-cluster-a.example.com:80
		// Extract domain from Host/SNI, lookup in services map, found it's a local service,
		// forward directly to local backend
		logging.Logf("[route] DECISION: handle locally - name=%s backend=%s client=%s", clientName, client.BackendAddr, remote)
		logging.Logf("[tunnel] local service direct forward name=%s backend=%s", clientName, client.BackendAddr)
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] local service direct forward (remote=%s name=%s backend=%s)", remote, clientName, client.BackendAddr)
		}

		// Metrics callback
		updateMetrics := func(clientName, protocol string, success bool, bytesTx, bytesRx int64, duration time.Duration) {
			if s.collector != nil {
				s.collector.UpdateConnectionMetrics(clientName, protocol, success, bytesTx, bytesRx, duration)
			}
		}

		// Use buffered connection to replay initial data
		bufConn := &proxy.BufferedConn{
			Conn: conn,
			Buf:  initial,
			Pos:  0,
		}

		s.forwardDirect(bufConn, conn, clientName, protocol, client.BackendAddr, cfg, updateMetrics)
		return
	}

	// Remote service - need to forward to other peer via tunnel
	// Design doc flow: Peer B receives request for test-cluster-a.example.com:80
	// Extract domain from Host/SNI, lookup in services map, found it's a service from Peer A
	// Generate unique proxy_id, send FORWARD command to Peer A

	logging.Logf("[route] DECISION: forward to next proxy - name=%s peer_id=%s remote_peer_addr=%s backend=%s client=%s",
		clientName, client.PeerID, client.IP, client.BackendAddr, remote)

	// Check connection is available for remote services
	if !client.Connected {
		logging.Logf("[tunnel] Remote client %s (peer_id=%s remote_peer_addr=%s) not connected - cannot forward", clientName, client.PeerID, client.IP)
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] remote client not connected (remote=%s name=%s peer_id=%s remote_peer_addr=%s)", remote, clientName, client.PeerID, client.IP)
		}
		if s.collector != nil {
			s.collector.RecordProxyError(clientName, "remote_not_connected")
		}
		// Return error response for HTTP requests
		if protocol == "http" || protocol == "http_connect" {
			errorMsg := fmt.Sprintf("HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\nService unavailable: peer %s not connected\n", len("Service unavailable: peer  not connected\n")+len(client.IP), client.IP)
			_, _ = conn.Write([]byte(errorMsg))
		}
		return
	}

	if client.Conn == nil {
		logging.Logf("[tunnel] ERROR: Remote client %s (peer_id=%s remote_peer_addr=%s) connection is nil - cannot forward", clientName, client.PeerID, client.IP)
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] remote client conn is nil (remote=%s name=%s peer_id=%s remote_peer_addr=%s)", remote, clientName, client.PeerID, client.IP)
		}
		// IMPORTANT: For unidirectional connections (Peer A has remote_peer_addr, Peer B doesn't):
		// - Peer A connects to Peer B (incoming connection from Peer B's perspective)
		// - Peer B can use this connection to send FORWARD to Peer A (TCP is bidirectional)
		// - But if client.Conn is nil, it means the connection wasn't stored correctly
		// This can happen if services were synced via SYNC but the connection wasn't associated
		logging.Logf("[tunnel] WARNING: Connection is nil for peer_id=%s. This may indicate a unidirectional connection issue.", client.PeerID)
		if s.collector != nil {
			s.collector.RecordProxyError(clientName, "remote_conn_nil")
		}
		// Return error response for HTTP requests
		if protocol == "http" || protocol == "http_connect" {
			errorMsg := fmt.Sprintf("HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\nService unavailable: peer %s connection lost\n", len("Service unavailable: peer  connection lost\n")+len(client.IP), client.IP)
			_, _ = conn.Write([]byte(errorMsg))
		}
		return
	}

	// Verify connection is still valid by checking remote address
	if client.Conn.RemoteAddr() == nil {
		logging.Logf("[tunnel] Remote client %s (peer_id=%s remote_peer_addr=%s) connection is closed - cannot forward", clientName, client.PeerID, client.IP)
		client.Connected = false
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] remote client conn is closed (remote=%s name=%s peer_id=%s remote_peer_addr=%s)", remote, clientName, client.PeerID, client.IP)
		}
		if s.collector != nil {
			s.collector.RecordProxyError(clientName, "remote_conn_closed")
		}
		// Return error response for HTTP requests
		if protocol == "http" || protocol == "http_connect" {
			errorMsg := fmt.Sprintf("HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\nService unavailable: peer %s connection closed\n", len("Service unavailable: peer  connection closed\n")+len(client.IP), client.IP)
			_, _ = conn.Write([]byte(errorMsg))
		}
		return
	}

	logging.Logf("[tunnel] remote service ready name=%s peer_id=%s remote_peer_addr=%s conn=%v connected=%t backend=%s",
		clientName, client.PeerID, client.IP, client.Conn != nil, client.Connected, client.BackendAddr)

	// Log selected backend details after route selection.
	if cfg != nil && cfg.Log.Level == "debug" {
		backend := client.BackendAddr
		if backend == "" && client.IP != "" {
			backend = net.JoinHostPort(client.IP, "80")
		}
		logging.Logf("[debug] selected backend (remote=%s name=%s protocol=%s backend=%s)", remote, clientName, protocol, backend)
		logging.Logf("[debug] starting tunnel forward (remote=%s name=%s protocol=%s backend=%s peer_id=%s remote_peer_addr=%s)",
			remote, clientName, protocol, client.BackendAddr, client.PeerID, client.IP)
	}

	// Metrics callback
	updateMetrics := func(clientName, protocol string, success bool, bytesTx, bytesRx int64, duration time.Duration) {
		if s.collector != nil {
			s.collector.UpdateConnectionMetrics(clientName, protocol, success, bytesTx, bytesRx, duration)
		}
	}

	// Use buffered connection to replay initial data
	bufConn := &proxy.BufferedConn{
		Conn: conn,
		Buf:  initial,
		Pos:  0,
	}

	// Forward via tunnel to remote peer
	// Design doc flow:
	// 1. Generate unique proxy_id
	// 2. Send FORWARD command: FORWARD:proxy_id:name:backend\n
	// 3. Wait for DATA connection with matching proxy_id
	// 4. Bridge data streams
	logging.Logf("[route] FORWARDING to next proxy - name=%s peer_id=%s remote_peer_addr=%s backend=%s protocol=%s client=%s",
		clientName, client.PeerID, client.IP, client.BackendAddr, protocol, remote)
	logging.Logf("[tunnel] forwarding via tunnel name=%s backend=%s peer_id=%s remote_peer_addr=%s protocol=%s",
		clientName, client.BackendAddr, client.PeerID, client.IP, protocol)
	s.forwardOnce(bufConn, conn, clientName, protocol, client, cfg, updateMetrics, "")
}

func (s *ProxyServer) logAcceptEOF(remote string, cfg *config.Config) {
	if cfg == nil || cfg.Log.Level != "debug" {
		return
	}
	now := time.Now()

	s.acceptEOFLock.Lock()
	defer s.acceptEOFLock.Unlock()

	// Log at most once per 5s; count suppressed events.
	const window = 5 * time.Second
	if !s.acceptEOFLastLogAt.IsZero() && now.Sub(s.acceptEOFLastLogAt) < window {
		s.acceptEOFSuppressed++
		return
	}

	if s.acceptEOFSuppressed > 0 && !s.acceptEOFLastLogAt.IsZero() {
		logging.Logf(
			"[accept][debug] initial read EOF (remote=%s) (suppressed=%d in last=%s)",
			remote,
			s.acceptEOFSuppressed,
			now.Sub(s.acceptEOFLastLogAt).Truncate(time.Second),
		)
	} else {
		logging.Logf("[accept][debug] initial read EOF (remote=%s)", remote)
	}

	s.acceptEOFSuppressed = 0
	s.acceptEOFLastLogAt = now
}

func (s *ProxyServer) forwardDirect(srcReader io.Reader, srcConn net.Conn, name, protocol, backendAddr string, cfg *config.Config, update func(string, string, bool, int64, int64, time.Duration)) {
	start := time.Now()
	remote := ""
	if srcConn != nil && srcConn.RemoteAddr() != nil {
		remote = srcConn.RemoteAddr().String()
	}

	// Always log direct forward start (not just debug)
	logging.Logf("[reverse] direct forward start name=%s protocol=%s backend=%s remote=%s", name, protocol, backendAddr, remote)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] direct forward start (remote=%s name=%s protocol=%s backend=%s)", remote, name, protocol, backendAddr)
	}

	if s.collector != nil {
		s.collector.IncActiveConnection(name)
		defer s.collector.DecActiveConnection(name)
	}

	dialTimeout := 30 * time.Second
	if cfg != nil {
		dialTimeout = cfg.GetDialTimeout()
	}
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] direct dial attempt (remote=%s backend=%s dial_timeout=%s)", remote, backendAddr, dialTimeout)
	}

	logging.Logf("[reverse] dialing backend name=%s backend=%s timeout=%s", name, backendAddr, dialTimeout)
	backendConn, err := net.DialTimeout("tcp", backendAddr, dialTimeout)
	if err != nil {
		logging.Logf("[reverse] dial failed name=%s backend=%s err=%v", name, backendAddr, err)
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] direct dial failed (remote=%s name=%s backend=%s err=%v)", remote, name, backendAddr, err)
		}
		if s.collector != nil {
			s.collector.RecordProxyError(name, "backend_dial_error")
		}
		if update != nil {
			update(name, protocol, false, 0, 0, time.Since(start))
		}
		return
	}
	defer backendConn.Close()
	local := ""
	if backendConn.LocalAddr() != nil {
		local = backendConn.LocalAddr().String()
	}
	peer := ""
	if backendConn.RemoteAddr() != nil {
		peer = backendConn.RemoteAddr().String()
	}
	logging.Logf("[reverse] dial connected name=%s backend=%s local=%s peer=%s", name, backendAddr, local, peer)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] direct dial connected (remote=%s backend=%s local=%s peer=%s)", remote, backendAddr, local, peer)
		logging.Logf("[debug] direct bridge start (remote=%s name=%s protocol=%s)", remote, name, protocol)
	}

	var bytesTx, bytesRx int64
	errCh := make(chan error, 2)

	go func() {
		n, err := io.Copy(backendConn, srcReader)
		bytesTx = n
		errCh <- err
	}()
	go func() {
		n, err := io.Copy(srcConn, backendConn)
		bytesRx = n
		errCh <- err
	}()

	err = <-errCh
	success := err == nil || err == io.EOF
	duration := time.Since(start)
	if !success {
		logging.Logf("[reverse] bridge error name=%s backend=%s bytes_tx=%d bytes_rx=%d duration=%s err=%v", name, backendAddr, bytesTx, bytesRx, duration, err)
		if s.collector != nil {
			s.collector.RecordProxyError(name, "backend_io_error")
		}
	} else {
		logging.Logf("[reverse] bridge done name=%s backend=%s bytes_tx=%d bytes_rx=%d duration=%s success=true", name, backendAddr, bytesTx, bytesRx, duration)
	}
	if update != nil {
		update(name, protocol, success, bytesTx, bytesRx, duration)
	}
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[reverse][debug] bridge done (name=%s protocol=%s backend=%s bytes_tx=%d bytes_rx=%d duration=%s success=%t err=%v)", name, protocol, backendAddr, bytesTx, bytesRx, duration, success, err)
	}
}

func (s *ProxyServer) forwardOnce(srcReader io.Reader, srcConn net.Conn, name, protocol string, client *types.ClientInfo, cfg *config.Config, update func(string, string, bool, int64, int64, time.Duration), backendOverride string) {
	start := time.Now()
	if s.collector != nil {
		s.collector.IncActiveConnection(name)
		defer s.collector.DecActiveConnection(name)
	}

	backendAddr := ""
	if backendOverride != "" {
		backendAddr = backendOverride
	} else if client != nil {
		backendAddr = client.BackendAddr
	}
	// Design requirement: no default value for backend address
	// If backend is empty, this is a configuration error
	if backendAddr == "" {
		logging.Logf("[tunnel] ERROR: backend address is empty for service name=%s peer_id=%s", name, client.PeerID)
		if s.collector != nil {
			s.collector.RecordProxyError(name, "empty_backend")
		}
		if update != nil {
			update(name, protocol, false, 0, 0, time.Since(start))
		}
		return
	}

	// Allocate an 8-digit random proxy-id for this request to avoid collisions.
	// Design doc: "generate 8-digit random proxy_id"
	proxyID := generateProxyID()
	remote := ""
	if srcConn != nil && srcConn.RemoteAddr() != nil {
		remote = srcConn.RemoteAddr().String()
	}
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] tunnel forward start (remote=%s name=%s protocol=%s backend=%s proxy_id=%s)", remote, name, protocol, backendAddr, proxyID)
	}

	ch := make(chan net.Conn, 1)
	s.pendingLock.Lock()
	s.pendingData[proxyID] = ch
	pendingCount := len(s.pendingData)
	if s.collector != nil {
		s.collector.SetPendingDataConnections(pendingCount)
	}
	s.pendingLock.Unlock()
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] created pending DATA channel (proxy_id=%s name=%s pending_count=%d)", proxyID, name, pendingCount)
	}

	cleanupPending := func() {
		s.pendingLock.Lock()
		// Only delete if it's still the same channel.
		if cur, ok := s.pendingData[proxyID]; ok && cur == ch {
			delete(s.pendingData, proxyID)
		}
		if s.collector != nil {
			s.collector.SetPendingDataConnections(len(s.pendingData))
		}
		s.pendingLock.Unlock()
	}
	defer cleanupPending()

	// Send FORWARD command on control connection.
	// Design doc format: "FORWARD:proxy_id:name:backend\n"
	// Example: "FORWARD:proxy_id:test-cluster-a.example.com:80\n"
	if client.ConnMu != nil {
		client.ConnMu.Lock()
		defer client.ConnMu.Unlock()
	}

	// Double-check connection is still valid before writing
	if client.Conn == nil {
		logging.Logf("[tunnel] connection is nil before sending FORWARD name=%s peer_id=%s", name, client.PeerID)
		client.Connected = false
		if s.collector != nil {
			s.collector.RecordProxyError(name, "forward_conn_nil")
		}
		if update != nil {
			update(name, protocol, false, 0, 0, time.Since(start))
		}
		return
	}

	// Verify connection is still valid by checking remote address
	// This is a quick check, but the connection might still be closed
	// We'll catch the actual error when writing
	remoteAddr := ""
	if client.Conn.RemoteAddr() != nil {
		remoteAddr = client.Conn.RemoteAddr().String()
	} else {
		logging.Logf("[tunnel] connection remote address is nil (connection closed) name=%s peer_id=%s", name, client.PeerID)
		client.Connected = false
		if s.collector != nil {
			s.collector.RecordProxyError(name, "forward_conn_closed")
		}
		if update != nil {
			update(name, protocol, false, 0, 0, time.Since(start))
		}
		return
	}

	// Format FORWARD command according to design doc
	// Format: FORWARD:proxy_id:name:backend_addr\n
	cmd := wire.FormatForward(proxyID, name, backendAddr)
	logging.Logf("[tunnel] SENDING FORWARD proxy_id=%s name=%s backend=%s peer_id=%s remote_peer_addr=%s client=%s",
		proxyID, name, backendAddr, client.PeerID, client.IP, remote)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] FORWARD command (proxy_id=%s cmd=%q peer_remote=%s)", proxyID, strings.TrimSpace(cmd), remoteAddr)
	}
	_, err := client.Conn.Write([]byte(cmd))
	if err != nil {
		logging.Logf("[tunnel] FORWARD send failed proxy_id=%s name=%s peer_id=%s err=%v", proxyID, name, client.PeerID, err)
		client.Connected = false
		// Unregister all services from this connection to prevent further attempts
		s.UnregisterClientsByConn(client.Conn)
		if s.collector != nil {
			s.collector.RecordProxyError(name, "forward_write_error")
		}
		if update != nil {
			update(name, protocol, false, 0, 0, time.Since(start))
		}
		return
	}
	logging.Logf("[tunnel] FORWARD sent successfully proxy_id=%s name=%s peer_id=%s", proxyID, name, client.PeerID)
	if s.collector != nil {
		s.collector.RecordForwardCommand(name)
	}
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] FORWARD command sent (proxy_id=%s client=%s name=%s backend=%s)", proxyID, remote, name, backendAddr)
	}

	// Wait for DATA on control connection (peer-to-peer: long connection)
	// For peer-to-peer communication, data is transmitted over the existing control connection
	// The receiving peer will send DATA:<proxy-id> on the control connection,
	// which will be detected by the control connection reader and passed to us via channel
	// Note: External client connections (srcConn) are short connections and will be closed after use
	var dataConn net.Conn
	dataTimeout := 30 * time.Second // Default timeout (increased from 5s to 30s)
	if cfg != nil {
		dataTimeout = cfg.GetConnectionTimeout()
	}
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] waiting for DATA on control connection (proxy_id=%s name=%s timeout=%s)", proxyID, name, dataTimeout)
	}
	select {
	case dataConn = <-ch:
		peer := ""
		if dataConn != nil && dataConn.RemoteAddr() != nil {
			peer = dataConn.RemoteAddr().String()
		}
		logging.Logf("[tunnel] DATA received on control connection name=%s proxy_id=%s data_peer=%s", name, proxyID, peer)
		if s.collector != nil {
			s.collector.RecordDataMatched(name)
		}
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] DATA received on control connection (remote=%s name=%s proxy_id=%s data_peer=%s)", remote, name, proxyID, peer)
			logging.Logf("[debug] bridge start (remote=%s name=%s protocol=%s proxy_id=%s)", remote, name, protocol, proxyID)
		}
	case <-time.After(dataTimeout):
		logging.Logf("[tunnel] DATA timeout on control connection proxy_id=%s name=%s peer_id=%s timeout=%s", proxyID, name, client.PeerID, dataTimeout)
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] DATA connection timeout (proxy_id=%s client=%s name=%s timeout=%s)", proxyID, remote, name, dataTimeout)
		}
		if s.collector != nil {
			s.collector.RecordDataTimeout(name)
			s.collector.RecordProxyError(name, "data_timeout")
		}
		if update != nil {
			update(name, protocol, false, 0, 0, time.Since(start))
		}
		return
	}
	// Don't close the control connection - it's still used for other commands
	// defer dataConn.Close() // REMOVED - don't close control connection

	logging.Logf("[tunnel] bridge start name=%s proxy_id=%s", name, proxyID)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] starting data bridge (remote=%s name=%s proxy_id=%s)", remote, name, proxyID)
	}

	var bytesTx, bytesRx int64
	errCh := make(chan error, 2)

	go func() {
		n, err := io.Copy(dataConn, srcReader) // client -> data connection (to peer)
		bytesTx = n
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] client->peer done (proxy_id=%s bytes=%d err=%v)", proxyID, n, err)
		}
		// Don't close control connection write side - it's still used for other commands
		// Only signal EOF by not writing more data
		errCh <- err
	}()
	go func() {
		n, err := io.Copy(srcConn, dataConn) // data connection (from peer) -> client
		bytesRx = n
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] peer->client done (proxy_id=%s bytes=%d err=%v)", proxyID, n, err)
		}
		// Close write side to signal EOF to client
		if tcpConn, ok := srcConn.(*net.TCPConn); ok {
			tcpConn.CloseWrite()
		}
		errCh <- err
	}()

	// Wait for both directions to complete
	err1 := <-errCh
	err2 := <-errCh

	// Take first non-EOF error
	err = err1
	if err == nil || err == io.EOF {
		err = err2
	}
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] bridge completed (remote=%s name=%s proxy_id=%s bytes_tx=%d bytes_rx=%d err1=%v err2=%v)", remote, name, proxyID, bytesTx, bytesRx, err1, err2)
	}

	success := err == nil || err == io.EOF
	if !success {
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] bridge error (remote=%s name=%s protocol=%s proxy_id=%s err=%v)", remote, name, protocol, proxyID, err)
		}
		logging.Logf("[tunnel] Proxy data error for %s: %v", name, err)
		if s.collector != nil {
			s.collector.RecordProxyError(name, "data_io_error")
		}
	}
	if update != nil {
		update(name, protocol, success, bytesTx, bytesRx, time.Since(start))
	}
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] bridge done (remote=%s name=%s protocol=%s proxy_id=%s bytes_tx=%d bytes_rx=%d duration=%s success=%t err=%v)", remote, name, protocol, proxyID, bytesTx, bytesRx, time.Since(start), success, err)
	}
}

// parseAllProxyPeerID parses ALL_PROXY environment variable to extract peer ID
// Format: socks5://peerid:port or http://peerid:port
// Returns the peer ID if found, empty string otherwise
func parseAllProxyPeerID() string {
	allProxy := os.Getenv("ALL_PROXY")
	if allProxy == "" {
		return ""
	}

	// Parse URL
	u, err := url.Parse(allProxy)
	if err != nil {
		logging.Logf("[route] failed to parse ALL_PROXY=%q: %v", allProxy, err)
		return ""
	}

	// Extract hostname (which should be the peer ID)
	peerID := u.Hostname()
	if peerID == "" {
		return ""
	}

	// Remove port if present (format: peerid:port)
	if idx := strings.Index(peerID, ":"); idx > 0 {
		peerID = peerID[:idx]
	}

	logging.Logf("[route] parsed ALL_PROXY=%q -> peer_id=%q", allProxy, peerID)
	return peerID
}

func generateProxyID() string {
	const max = 100000000
	n, err := rand.Int(rand.Reader, big.NewInt(max))
	if err != nil {
		// Fallback to time-based value to avoid blocking the request path.
		return fmt.Sprintf("%08d", time.Now().UnixNano()%max)
	}
	return fmt.Sprintf("%08d", n.Int64())
}

// clientsDebugSnapshot returns a stable snapshot of current clients on this server instance.
// Format: name(up|down), sorted by name.
func (s *ProxyServer) clientsDebugSnapshot() string {
	if len(s.clients) == 0 {
		return "<empty>"
	}

	items := make([]string, 0, len(s.clients))
	for name, c := range s.clients {
		state := "down"
		if c != nil && c.Connected {
			state = "up"
		}
		items = append(items, fmt.Sprintf("%s(%s)", name, state))
	}
	sort.Strings(items)
	return strings.Join(items, ",")
}

// getDefaultClientName gets the default client name
// Returns the first connected client's name
func (s *ProxyServer) getDefaultClientName() string {
	// Return first connected client's name
	for name, client := range s.clients {
		if client.Connected {
			return name
		}
	}
	return ""
}
