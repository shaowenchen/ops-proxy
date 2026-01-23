package server

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"net"
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
		logging.Logf("[request][debug] new connection (remote=%s)", remote)
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
			logging.Logf("[request][debug] registration/data request (remote=%s bytes=%d)", remote, len(payload))
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
		logging.Logf("[request][debug] proxy request (remote=%s bytes=%d read_timeout=%s)", remote, n, readTimeout)
	}

	// Detect protocol type and route
	getDefaultClient := func() string {
		return s.getDefaultClientName()
	}
	extractHost := func(data []byte) string {
		host := proxy.ExtractHostFromHTTP(data)
		if cfg != nil && cfg.Log.Level == "debug" && host != "" {
			logging.Logf("[request][debug] extracted HTTP Host (remote=%s host=%q)", remote, host)
		}
		return host
	}
	extractSNI := func(data []byte) string {
		sni := proxy.ExtractSNI(data)
		if cfg != nil && cfg.Log.Level == "debug" && sni != "" {
			logging.Logf("[request][debug] extracted TLS SNI (remote=%s sni=%q)", remote, sni)
		}
		return sni
	}
	protocol, clientName := proxy.DetectProtocolAndRoute(initial, getDefaultClient, extractHost, extractSNI)
	extractedName := clientName

	// Log protocol and extracted name immediately after detection
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[request][debug] protocol detected (remote=%s protocol=%s extracted_name=%q)", remote, protocol, extractedName)
	}

	// Forward proxy (HTTP CONNECT).
	// Always supported by default: if CONNECT is detected, proxy to its target.
	if protocol == "http_connect" {
		host, port, ok := proxy.ExtractConnectHostPort(initial)
		if !ok {
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[request][debug] HTTP CONNECT parse failed (remote=%s)", remote)
			}
			logging.Logf("HTTP CONNECT detected but failed to parse target")
			return
		}
		targetAddr := net.JoinHostPort(host, port)

		egressName := s.getDefaultClientName()
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] HTTP CONNECT request (remote=%s target=%s default_client=%s)", remote, targetAddr, egressName)
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

		if egressName != "" {
			if c := s.GetClient(egressName); c != nil && c.Connected && c.Conn != nil {
				if cfg != nil && cfg.Log.Level == "debug" {
					logging.Logf("[request][debug] CONNECT tunnel selected (remote=%s target=%s egress_client=%s)", remote, targetAddr, egressName)
				}
				s.forwardOnce(bufConn, conn, egressName, "forward", c, cfg, updateMetrics, targetAddr)
				return
			}
		}

		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] CONNECT direct dial (remote=%s target=%s)", remote, targetAddr)
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
				logging.Logf("[request][debug] protocol detection (remote=%s protocol=http host_found=false bytes=%d preview=%q)", remote, n, string(initial[:maxLen]))
			} else if protocol == "https" {
				maxLen = 20
				if n < maxLen {
					maxLen = n
				}
				logging.Logf("[request][debug] protocol detection (remote=%s protocol=https sni_found=false bytes=%d preview=%x)", remote, n, initial[:maxLen])
			} else {
				maxLen = 20
				if n < maxLen {
					maxLen = n
				}
				logging.Logf("[request][debug] protocol detection (remote=%s protocol=tcp bytes=%d preview=%x)", remote, n, initial[:maxLen])
			}
		}
	}

	// If clientName is empty (e.g., Host header not found or SNI extraction failed), use default client
	if clientName == "" {
		clientName = s.getDefaultClientName()
		if clientName == "" {
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[request][debug] route selection failed (remote=%s protocol=%s reason=no_host_sni_and_no_default)", remote, protocol)
			}
			logging.Logf("[route] select_failed protocol=%s reason=no_host_sni_and_no_default", protocol)
			// Return error response for HTTP requests
			if protocol == "http" {
				errorMsg := "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 40\r\n\r\nBad Request: No Host header or SNI found\n"
				_, _ = conn.Write([]byte(errorMsg))
			}
			return
		}
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] route selected (remote=%s name=%q protocol=%s reason=default_fallback)", remote, clientName, protocol)
		}
		logging.Logf("[route] selected name=%q protocol=%s reason=default_fallback", clientName, protocol)
	} else {
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] route selected (remote=%s name=%q protocol=%s reason=extracted extracted=%q)", remote, clientName, protocol, extractedName)
		}
		logging.Logf("[route] selected name=%q protocol=%s reason=extracted", clientName, protocol)
	}

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
		logging.Logf("[request][debug] svc candidates (name=%q items=%s)", clientName, candidates)
	}

	// Debug: show extracted domain, selected client, and current service pool on this server instance.
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[request][debug] route details (remote=%s protocol=%s extracted=%q selected=%q services=[%s])", remote, protocol, extractedName, clientName, s.servicesDebugSnapshot())
		// Print full service table in debug mode
		s.logServicesTable(cfg)
	}

	// Get client and immediately log service type (local or remote) for visibility
	logging.Logf("[route] calling GetClient name=%q", clientName)
	var client *types.ClientInfo
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
			logging.Logf("[request][debug] client not found (remote=%s name=%s protocol=%s)", remote, clientName, protocol)
		}
		if s.collector != nil {
			s.collector.RecordProxyError(clientName, "no_client")
		}
		// Try default client as fallback
		defaultClientName := s.getDefaultClientName()
		if defaultClientName != "" && defaultClientName != clientName {
			client = s.GetClient(defaultClientName)
			if client != nil {
				logging.Logf("[tunnel] Using default client %s as fallback", defaultClientName)
				if cfg != nil && cfg.Log.Level == "debug" {
					logging.Logf("[request][debug] using default client fallback (remote=%s original=%s fallback=%s)", remote, clientName, defaultClientName)
				}
				clientName = defaultClientName
			}
		}
		if client == nil {
			logging.Logf("[tunnel] No client available for proxy")
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[request][debug] no client available (remote=%s protocol=%s)", remote, protocol)
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
			logging.Logf("[request][debug] local service direct forward (remote=%s name=%s backend=%s)", remote, clientName, client.BackendAddr)
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
			logging.Logf("[request][debug] remote client not connected (remote=%s name=%s peer_id=%s remote_peer_addr=%s)", remote, clientName, client.PeerID, client.IP)
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
		logging.Logf("[tunnel] Remote client %s (peer_id=%s remote_peer_addr=%s) connection is nil - cannot forward", clientName, client.PeerID, client.IP)
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] remote client conn is nil (remote=%s name=%s peer_id=%s remote_peer_addr=%s)", remote, clientName, client.PeerID, client.IP)
		}
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
			logging.Logf("[request][debug] remote client conn is closed (remote=%s name=%s peer_id=%s remote_peer_addr=%s)", remote, clientName, client.PeerID, client.IP)
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
		logging.Logf("[request][debug] selected backend (remote=%s name=%s protocol=%s backend=%s)", remote, clientName, protocol, backend)
		logging.Logf("[request][debug] starting tunnel forward (remote=%s name=%s protocol=%s backend=%s peer_id=%s remote_peer_addr=%s)", 
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
		logging.Logf("[request][debug] direct forward start (remote=%s name=%s protocol=%s backend=%s)", remote, name, protocol, backendAddr)
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
		logging.Logf("[request][debug] direct dial attempt (remote=%s backend=%s dial_timeout=%s)", remote, backendAddr, dialTimeout)
	}

	logging.Logf("[reverse] dialing backend name=%s backend=%s timeout=%s", name, backendAddr, dialTimeout)
	backendConn, err := net.DialTimeout("tcp", backendAddr, dialTimeout)
	if err != nil {
		logging.Logf("[reverse] dial failed name=%s backend=%s err=%v", name, backendAddr, err)
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] direct dial failed (remote=%s name=%s backend=%s err=%v)", remote, name, backendAddr, err)
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
		logging.Logf("[request][debug] direct dial connected (remote=%s backend=%s local=%s peer=%s)", remote, backendAddr, local, peer)
		logging.Logf("[request][debug] direct bridge start (remote=%s name=%s protocol=%s)", remote, name, protocol)
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
		logging.Logf("[request][debug] tunnel forward start (remote=%s name=%s protocol=%s backend=%s proxy_id=%s)", remote, name, protocol, backendAddr, proxyID)
	}

	ch := make(chan net.Conn, 1)
	s.pendingLock.Lock()
	s.pendingData[proxyID] = ch
	if s.collector != nil {
		s.collector.SetPendingDataConnections(len(s.pendingData))
	}
	s.pendingLock.Unlock()

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
		logging.Logf("[request][debug] FORWARD command (proxy_id=%s cmd=%q peer_remote=%s)", proxyID, strings.TrimSpace(cmd), remoteAddr)
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
		logging.Logf("[request][debug] FORWARD command sent (proxy_id=%s client=%s name=%s backend=%s)", proxyID, remote, name, backendAddr)
	}

	// Wait for client to connect back with DATA:<proxy-id> on registration port.
	var dataConn net.Conn
	dataTimeout := 5 * time.Second
	if cfg != nil {
		dataTimeout = cfg.GetConnectionTimeout()
	}
	select {
	case dataConn = <-ch:
		peer := ""
		if dataConn != nil && dataConn.RemoteAddr() != nil {
			peer = dataConn.RemoteAddr().String()
		}
		logging.Logf("[tunnel] DATA connection established name=%s proxy_id=%s data_peer=%s", name, proxyID, peer)
		if s.collector != nil {
			s.collector.RecordDataMatched(name)
		}
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] DATA connection established (remote=%s name=%s proxy_id=%s data_peer=%s)", remote, name, proxyID, peer)
			logging.Logf("[request][debug] bridge start (remote=%s name=%s protocol=%s proxy_id=%s)", remote, name, protocol, proxyID)
		}
	case <-time.After(dataTimeout):
		logging.Logf("[tunnel] DATA timeout proxy_id=%s name=%s peer_id=%s timeout=%s", proxyID, name, client.PeerID, dataTimeout)
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] DATA connection timeout (proxy_id=%s client=%s name=%s timeout=%s)", proxyID, remote, name, dataTimeout)
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
	defer dataConn.Close()

	logging.Logf("[tunnel] bridge start name=%s proxy_id=%s", name, proxyID)

	var bytesTx, bytesRx int64
	errCh := make(chan error, 2)

	go func() {
		n, err := io.Copy(dataConn, srcReader) // client -> data connection (to peer)
		bytesTx = n
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] client->peer done (proxy_id=%s bytes=%d err=%v)", proxyID, n, err)
		}
		// Close write side to signal EOF to peer
		if tcpConn, ok := dataConn.(*net.TCPConn); ok {
			tcpConn.CloseWrite()
		}
		errCh <- err
	}()
	go func() {
		n, err := io.Copy(srcConn, dataConn) // data connection (from peer) -> client
		bytesRx = n
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] peer->client done (proxy_id=%s bytes=%d err=%v)", proxyID, n, err)
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

	success := err == nil || err == io.EOF
	if !success {
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] bridge error (remote=%s name=%s protocol=%s proxy_id=%s err=%v)", remote, name, protocol, proxyID, err)
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
		logging.Logf("[request][debug] bridge done (remote=%s name=%s protocol=%s proxy_id=%s bytes_tx=%d bytes_rx=%d duration=%s success=%t err=%v)", remote, name, protocol, proxyID, bytesTx, bytesRx, time.Since(start), success, err)
	}
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
	s.clientsLock.RLock()
	defer s.clientsLock.RUnlock()

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
	s.clientsLock.RLock()
	defer s.clientsLock.RUnlock()

	// Return first connected client's name
	for name, client := range s.clients {
		if client.Connected {
			return name
		}
	}
	return ""
}
