package client

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ops-proxy/pkg/config"
	"github.com/ops-proxy/pkg/logging"
	"github.com/ops-proxy/pkg/mesh"
	"github.com/ops-proxy/pkg/protocol"
	"github.com/ops-proxy/pkg/routing"
)

// Run starts the peer connection client
// Supports connecting to multiple peers and registering local services
// Each service must have a domain name for routing (matched against Host/SNI)
// proxyServer is optional; if provided, SYNC commands will register services to it
func Run(cfg *config.Config, serverAddr, clientName, backendAddr string, proxyServer interface {
	RegisterClientByName(name, ip, backendAddr string, conn net.Conn)
	LogServicesTable()
}) error {
	// Get peer addresses and service addresses from config
	var peerAddrs []string
	var serviceAddr string

	if cfg != nil {
		// Use REMOTE_PEER_ADDR / SERVICE_ADDR if available
		if cfg.Peer.RemotePeerAddr != "" {
			// REMOTE_PEER_ADDR can be comma-separated list
			peerAddrs = strings.Split(cfg.Peer.RemotePeerAddr, ",")
			for i := range peerAddrs {
				peerAddrs[i] = strings.TrimSpace(peerAddrs[i])
			}
		} else if cfg.Peer.PeerAddr != "" {
			// Legacy: use PeerAddr as remote peer
			peerAddrs = strings.Split(cfg.Peer.PeerAddr, ",")
			for i := range peerAddrs {
				peerAddrs[i] = strings.TrimSpace(peerAddrs[i])
			}
		}
		if cfg.Peer.ServiceAddr != "" {
			serviceAddr = cfg.Peer.ServiceAddr
		}

		// Fallback to legacy fields
		if len(peerAddrs) == 0 {
			if serverAddr != "" {
				peerAddrs = []string{serverAddr}
			} else if cfg.Peer.ServerAddr != "" {
				peerAddrs = []string{cfg.Peer.ServerAddr}
			}
		}
		if serviceAddr == "" {
			if backendAddr != "" {
				serviceAddr = backendAddr
			} else if cfg.Peer.BackendAddr != "" {
				serviceAddr = cfg.Peer.BackendAddr
			}
		}

	} else {
		// Use command line parameters
		if serverAddr != "" {
			peerAddrs = []string{serverAddr}
		}
		if backendAddr != "" {
			serviceAddr = backendAddr
		}
	}

	if len(peerAddrs) == 0 {
		if serviceAddr != "" {
			logging.Logf("SERVICE_ADDR is set but no REMOTE_PEER_ADDR configured; skipping registration")
		}
		return fmt.Errorf("peer address is required (use REMOTE_PEER_ADDR env var, CLIENT_SERVER_ADDR env var, or --server-addr flag)")
	}
	if serviceAddr == "" {
		return fmt.Errorf("service address is required (use SERVICE_ADDR env var, CLIENT_BACKEND_ADDR env var, or config file)")
	}

	// Expand peer addresses (resolve hostnames to all A/AAAA records when possible)
	peerAddrs = expandPeerAddrs(peerAddrs, cfg)

	// Parse service addresses to get registrations
	registrations := parseServiceAddresses(cfg, serviceAddr, clientName)
	if len(registrations) == 0 {
		return fmt.Errorf("no valid service addresses found (use SERVICE_ADDR env var or CLIENT_BACKEND_ADDR)")
	}

	logging.Logf("Peer will register %d service(s) to %d peer(s)", len(registrations), len(peerAddrs))
	for _, reg := range registrations {
		logging.Logf("  - %s -> %s", reg.name, reg.backendAddr)
	}

	// Connect to each peer in parallel
	var wg sync.WaitGroup
	for _, peerAddr := range peerAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			if err := connectToPeer(addr, registrations, cfg, proxyServer); err != nil {
				logging.Logf("Peer connection to %s failed: %v", addr, err)
			}
		}(peerAddr)
	}

	// Wait for all peer connections (they will reconnect on their own)
	wg.Wait()
	return fmt.Errorf("all peer connections closed")
}

// parseServiceAddresses parses SERVICE_ADDR string into registrations
// Supports three formats: domain:port, name:host:port, name:ip:port
// Example: "example-cluster.example.com:6443,example-app:example-app.default.svc.cluster.local:80"
func parseServiceAddresses(cfg *config.Config, serviceAddr, clientName string) []struct {
	name        string
	backendAddr string
} {
	registrations := []struct {
		name        string
		backendAddr string
	}{}

	// Prioritize backends list format from config file
	if cfg != nil && len(cfg.Peer.Backends) > 0 {
		for _, backend := range cfg.Peer.Backends {
			if backend.Address != "" {
				name := backend.Name
				if name == "" {
					if clientName == "" {
						clientName = getDefaultClientName()
					}
					name = clientName
				}
				registrations = append(registrations, struct {
					name        string
					backendAddr string
				}{
					name:        name,
					backendAddr: backend.Address,
				})
			}
		}
	}

	// If list format is not used, parse string format
	if len(registrations) == 0 && serviceAddr != "" {
		if clientName == "" {
			clientName = getDefaultClientName()
		}
		parsed := routing.ParseBackendAddrString(serviceAddr, clientName)
		for _, b := range parsed {
			if b.Name == "" || b.Address == "" {
				continue
			}
			registrations = append(registrations, struct {
				name        string
				backendAddr string
			}{
				name:        b.Name,
				backendAddr: b.Address,
			})
		}
	}

	return registrations
}

func expandPeerAddrs(addrs []string, cfg *config.Config) []string {
	out := make([]string, 0, len(addrs))
	seen := make(map[string]struct{}, len(addrs))

	addUnique := func(addr string) {
		if addr == "" {
			return
		}
		if _, ok := seen[addr]; ok {
			return
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}

	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil || host == "" || port == "" {
			addUnique(addr)
			continue
		}
		if net.ParseIP(host) != nil {
			addUnique(addr)
			continue
		}

		ips, err := net.LookupHost(host)
		if err != nil || len(ips) == 0 {
			addUnique(addr)
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[request][debug] peer addr resolve failed (addr=%s err=%v)", addr, err)
			}
			continue
		}
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[request][debug] peer addr resolved (addr=%s ips=%s)", addr, strings.Join(ips, ","))
		}
		for _, ip := range ips {
			addUnique(net.JoinHostPort(ip, port))
		}
	}

	return out
}

// connectToPeer connects to a single peer and registers services
func connectToPeer(peerAddr string, registrations []struct {
	name        string
	backendAddr string
}, cfg *config.Config, proxyServer interface {
	RegisterClientByName(name, ip, backendAddr string, conn net.Conn)
	LogServicesTable()
}) error {
	return mesh.RunPeerLink(peerAddr, cfg, func(link mesh.LinkInfo) error {
		conn := link.Conn
		logging.Logf("Connected to peer %s", link.Addr)

		// Build registration message with peer_id and peer_addr
		regs := make([]protocol.Registration, 0, len(registrations))
		peerID := logging.GetPeerID()
		
		// Get peer's real bind address for DATA connections
		// Use POD_IP:BIND_PORT if available, otherwise use bind_addr from config
		peerAddr := getPeerBindAddr(cfg)
		
		logging.Logf("[client] preparing registration peer_id=%s peer_addr=%s services=%d", peerID, peerAddr, len(registrations))
		
		for _, reg := range registrations {
			regs = append(regs, protocol.Registration{Name: reg.name, Backend: reg.backendAddr})
		}
		registration := protocol.FormatRegisterWithPeerID(peerID, peerAddr, regs)
		
		if cfg != nil && cfg.Log.Level == "debug" {
			items := make([]string, 0, len(regs))
			for _, r := range regs {
				items = append(items, fmt.Sprintf("%s->%s", r.Name, r.Backend))
			}
			logging.Logf("[request][debug] sending registration (peer=%s peer_id=%s peer_addr=%s count=%d items=%s msg=%q)", link.Addr, peerID, peerAddr, len(regs), strings.Join(items, ","), strings.TrimSpace(registration))
		}

		_, err := conn.Write([]byte(registration))
		if err != nil {
			return fmt.Errorf("failed to send registration to %s: %w", link.Addr, err)
		}

		// Read acknowledgment using bufio.Reader to handle multiple lines (OK + SYNC)
		reader := bufio.NewReader(conn)
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		ackLine, err := reader.ReadString('\n')
		conn.SetReadDeadline(time.Time{})
		if err != nil {
			return fmt.Errorf("failed to read acknowledgment from %s: %w", link.Addr, err)
		}

		ackLine = strings.TrimSpace(ackLine)
		if !strings.HasPrefix(ackLine, "OK") {
			return fmt.Errorf("registration failed to %s: %s", link.Addr, ackLine)
		}

		logging.Logf("Successfully registered %d service(s) to peer %s", len(registrations), link.Addr)

		// Server may send SYNC immediately after OK, try to read it (non-blocking with short timeout)
		// If SYNC is not available yet, it will be read in handleConnection loop
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		syncLine, syncErr := reader.ReadString('\n')
		conn.SetReadDeadline(time.Time{})
		if syncErr == nil {
			syncLine = strings.TrimSpace(syncLine)
			if regs, ok := protocol.ParseSyncLine(syncLine); ok {
				// Handle SYNC command immediately
				if proxyServer != nil {
					// Extract IP from peerAddr
					serverIP := link.Addr
					if idx := strings.LastIndex(link.Addr, ":"); idx > 0 {
						serverIP = link.Addr[:idx]
					}
					// Also try to get IP from connection's remote address
					if conn != nil && conn.RemoteAddr() != nil {
						if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
							serverIP = tcpAddr.IP.String()
						}
					}
					logging.Logf("[registry] received SYNC from %s (count=%d conn=%v)", serverIP, len(regs), conn != nil)
					for _, reg := range regs {
						if strings.TrimSpace(reg.Name) == "" || strings.TrimSpace(reg.Backend) == "" {
							continue
						}
						if cfg != nil && cfg.Log.Level == "debug" {
							logging.Logf("[debug] sync service name=%s backend=%s from=%s", reg.Name, reg.Backend, serverIP)
						}
						// Pass the actual connection so services can be forwarded
						proxyServer.RegisterClientByName(reg.Name, serverIP, reg.Backend, conn)
					}
					logging.Logf("[registry] synced %d service(s) from peer %s", len(regs), serverIP)
					proxyServer.LogServicesTable()
				}
			} else if !strings.HasPrefix(syncLine, "OK") && syncLine != "" {
				// If it's not SYNC and not OK, log it (might be FORWARD or other command)
				if cfg != nil && cfg.Log.Level == "debug" {
					logging.Logf("[debug] received unexpected line after OK: %q", syncLine)
				}
			}
		}
		// If SYNC was not received yet, it will be handled in handleConnection loop

		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[registry] services peer=%s svc_list:", link.Addr)
			logging.Logf("[registry] services peer=%s | peer | name | backend |", link.Addr)
			logging.Logf("[registry] services peer=%s | ---- | ---- | ------- |", link.Addr)
			for _, reg := range registrations {
				logging.Logf("[registry] services peer=%s | %s | %s | %s |", link.Addr, link.Addr, reg.name, reg.backendAddr)
			}
		}

		// Get heartbeat interval
		heartbeatInterval := 30 * time.Second
		if cfg != nil {
			heartbeatInterval = cfg.GetHeartbeatInterval()
		}

		// Re-register periodically
		stopReg := make(chan struct{})
		go func() {
			ticker := time.NewTicker(heartbeatInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					_, _ = conn.Write([]byte(registration))
					if cfg != nil && cfg.Log.Level == "debug" {
						logging.Logf("[request][debug] re-registration sent (peer=%s count=%d)", link.Addr, len(regs))
					}
				case <-stopReg:
					return
				}
			}
		}()

		// Handle control messages (use the same reader to continue reading)
		err = handleConnectionWithReader(conn, reader, link.Addr, cfg, proxyServer)
		close(stopReg)
		if err != nil {
			logging.Logf("Connection to %s error: %v, will reconnect", link.Addr, err)
		}

		return err
	})
}

// getDefaultClientName gets the default client name
func getDefaultClientName() string {
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		podName = os.Getenv("HOSTNAME")
	}
	if podName != "" {
		return podName
	}
	return "client-unknown"
}

// handleConnectionWithReader handles client connection with an existing reader
// This allows continuing to read from the same connection after initial registration
func handleConnectionWithReader(conn net.Conn, reader *bufio.Reader, serverAddress string, cfg *config.Config, proxyServer interface {
	RegisterClientByName(name, ip, backendAddr string, conn net.Conn)
	LogServicesTable()
}) error {
	// Read timeout for control connection. Client will re-register every 30s,
	// so the server should be sending either OK/ERROR responses or FORWARD commands.
	readTimeout := 60 * time.Second

	for {
		conn.SetReadDeadline(time.Now().Add(readTimeout))

		// Read line (up to \n)
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read error: %v", err)
		}

		line = protocol.TrimLine(line)

		if line == "" {
			continue
		}

		// Ignore periodic REGISTER acks
		if strings.HasPrefix(line, "OK") {
			continue
		}
		if strings.HasPrefix(line, "ERROR") {
			logging.Logf("Server error: %s", line)
			continue
		}

		if proxyID, name, backendAddr, ok := protocol.ParseForwardLine(line); ok {
			logging.Logf("[client] RECEIVED FORWARD proxy_id=%s name=%s backend=%s from=%s", proxyID, name, backendAddr, serverAddress)
			if cfg != nil && cfg.Log.Level == "debug" {
				logging.Logf("[debug] recv FORWARD proxy_id=%s name=%s backend=%s", proxyID, name, backendAddr)
			}
			// Run forwarding concurrently; control connection must keep reading more commands.
			go func(pid, n, backend string) {
				logging.Logf("[client] handling FORWARD proxy_id=%s name=%s backend=%s", pid, n, backend)
				if err := handleForwardOnce(serverAddress, pid, n, backend, cfg); err != nil {
					logging.Logf("[client] Forward failed proxy_id=%s name=%s err=%v", pid, n, err)
				} else {
					logging.Logf("[client] Forward succeeded proxy_id=%s name=%s", pid, n)
				}
			}(proxyID, name, backendAddr)
		} else if regs, ok := protocol.ParseSyncLine(line); ok {
			// Handle SYNC command: register services from peer
			if proxyServer != nil {
				// Extract IP from serverAddress (host:port -> host)
				serverIP := serverAddress
				if idx := strings.LastIndex(serverAddress, ":"); idx > 0 {
					serverIP = serverAddress[:idx]
				}
				// Also try to get IP from connection's remote address
				if conn != nil && conn.RemoteAddr() != nil {
					if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
						serverIP = tcpAddr.IP.String()
					}
				}
				logging.Logf("[registry] received SYNC from %s (count=%d conn=%v)", serverIP, len(regs), conn != nil)
				for _, reg := range regs {
					if strings.TrimSpace(reg.Name) == "" || strings.TrimSpace(reg.Backend) == "" {
						continue
					}
					if cfg != nil && cfg.Log.Level == "debug" {
						logging.Logf("[debug] sync service name=%s backend=%s from=%s", reg.Name, reg.Backend, serverIP)
					}
					// Pass the actual connection so services can be forwarded
					proxyServer.RegisterClientByName(reg.Name, serverIP, reg.Backend, conn)
				}
				if cfg != nil && cfg.Log.Level == "debug" {
					logging.Logf("[debug] synced %d service(s) from peer %s", len(regs), serverIP)
				}
				// Print service table after sync
				proxyServer.LogServicesTable()
			}
		} else {
			logging.Logf("Unknown command: %s", line)
		}
	}
}

// handleConnection handles client connection, keeps connection alive and handles heartbeat
// Also handles forward requests from server
// This is a wrapper that creates a new reader (for backward compatibility)
func handleConnection(conn net.Conn, serverAddress string, cfg *config.Config, proxyServer interface {
	RegisterClientByName(name, ip, backendAddr string, conn net.Conn)
	LogServicesTable()
}) error {
	reader := bufio.NewReader(conn)
	return handleConnectionWithReader(conn, reader, serverAddress, cfg, proxyServer)
}

// handleForwardOnce handles a single forward request.
// It opens a DATA connection back to server and bridges it with the backend connection.
func handleForwardOnce(serverAddress, proxyID, name, backendAddr string, cfg *config.Config) error {
	dialTimeout := 5 * time.Second
	if cfg != nil {
		dialTimeout = cfg.GetDialTimeout()
	}

	recordClientForwardStart(name)

	// Step 1: Connect to backend
	logging.Logf("[client] connecting to backend proxy_id=%s backend=%s", proxyID, backendAddr)
	backendConn, err := net.DialTimeout("tcp", backendAddr, dialTimeout)
	if err != nil {
		recordClientForwardFail(name, "backend_dial_error")
		logging.Logf("[client] backend dial failed proxy_id=%s backend=%s err=%v", proxyID, backendAddr, err)
		return fmt.Errorf("dial backend %s: %w", backendAddr, err)
	}
	defer backendConn.Close()
	logging.Logf("[client] backend connected proxy_id=%s backend=%s local=%s", proxyID, backendAddr, backendConn.LocalAddr())

	// Step 2: Connect back to server for DATA channel
	logging.Logf("[client] connecting DATA channel proxy_id=%s server=%s", proxyID, serverAddress)
	dataConn, err := net.DialTimeout("tcp", serverAddress, dialTimeout)
	if err != nil {
		recordClientForwardFail(name, "data_dial_error")
		logging.Logf("[client] DATA dial failed proxy_id=%s server=%s err=%v", proxyID, serverAddress, err)
		return fmt.Errorf("dial server data %s: %w", serverAddress, err)
	}
	defer dataConn.Close()

	// Step 3: Send DATA header to identify this connection
	if _, err := dataConn.Write([]byte(protocol.FormatData(proxyID))); err != nil {
		recordClientForwardFail(name, "data_header_error")
		logging.Logf("[client] DATA header send failed proxy_id=%s err=%v", proxyID, err)
		return fmt.Errorf("send DATA header: %w", err)
	}
	logging.Logf("[client] DATA channel established proxy_id=%s", proxyID)
	if cfg != nil && cfg.Log.Level == "debug" {
		logging.Logf("[debug] proxy_id=%s data_connected local=%s remote=%s", proxyID, dataConn.LocalAddr(), dataConn.RemoteAddr())
	}

	logging.Logf("[client] bridge start proxy_id=%s name=%s backend=%s", proxyID, name, backendAddr)

	// Bridge data in both directions
	errCh := make(chan error, 2)
	var bytesToServer, bytesToBackend int64
	
	go func() {
		n, e := io.Copy(dataConn, backendConn) // backend -> server
		bytesToServer = n
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] proxy_id=%s backend->server done bytes=%d err=%v", proxyID, n, e)
		}
		// Close write side to signal EOF to server
		if tcpConn, ok := dataConn.(*net.TCPConn); ok {
			tcpConn.CloseWrite()
		}
		errCh <- e
	}()
	
	go func() {
		n, e := io.Copy(backendConn, dataConn) // server -> backend
		bytesToBackend = n
		if cfg != nil && cfg.Log.Level == "debug" {
			logging.Logf("[debug] proxy_id=%s server->backend done bytes=%d err=%v", proxyID, n, e)
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
	
	logging.Logf("[client] bridge done proxy_id=%s name=%s bytes_to_server=%d bytes_to_backend=%d", proxyID, name, bytesToServer, bytesToBackend)
	
	// Return first non-nil error
	if err1 != nil && err1 != io.EOF {
		recordClientForwardFail(name, "io_error")
		return err1
	}
	if err2 != nil && err2 != io.EOF {
		recordClientForwardFail(name, "io_error")
		return err2
	}
	
	recordClientForwardSuccess(name)
	return nil
}

// getPeerBindAddr returns the peer's real bind address for DATA connections
// Priority: POD_IP:port > hostname:port > bind_addr from config
func getPeerBindAddr(cfg *config.Config) string {
	// Get bind port from config
	bindAddr := ":6443"
	if cfg != nil && cfg.Peer.BindAddr != "" {
		bindAddr = cfg.Peer.BindAddr
	}
	
	// Extract port from bind_addr
	_, port, err := net.SplitHostPort(bindAddr)
	if err != nil {
		// If bind_addr is not in host:port format, use it as-is
		port = "6443"
	}
	
	// Try to get POD_IP (Kubernetes environment)
	podIP := os.Getenv("POD_IP")
	if podIP != "" {
		return net.JoinHostPort(podIP, port)
	}
	
	// Try to get hostname and resolve to IP
	hostname := os.Getenv("HOSTNAME")
	if hostname == "" {
		hostname, _ = os.Hostname()
	}
	
	if hostname != "" {
		// Try to resolve hostname to IP
		ips, err := net.LookupHost(hostname)
		if err == nil && len(ips) > 0 {
			// Use first IP
			return net.JoinHostPort(ips[0], port)
		}
		// If resolution fails, use hostname directly
		return net.JoinHostPort(hostname, port)
	}
	
	// Fallback: use bind_addr from config
	return bindAddr
}
