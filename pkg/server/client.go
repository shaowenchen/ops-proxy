package server

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ops-proxy/pkg/config"
	"github.com/ops-proxy/pkg/logging"
	"github.com/ops-proxy/pkg/routing"
	"github.com/ops-proxy/pkg/types"
)

// RegisterClientByName registers a client by name
// Uses name as key, supports multiple names mapping to different backends
// If the same name is registered multiple times, the later one overwrites the previous (using the same connection)
func (s *ProxyServer) RegisterClientByName(name, ip, backendAddr string, conn net.Conn) {
	s.RegisterClientByNameWithPeerID(name, ip, backendAddr, conn, "")
}

// RegisterClientByNameWithPeerID registers a client with explicit peer ID
func (s *ProxyServer) RegisterClientByNameWithPeerID(name, ip, backendAddr string, conn net.Conn, peerID string) {
	s.RegisterClientByNameWithPeerInfo(name, ip, backendAddr, conn, peerID, "")
}

// RegisterClientByNameWithPeerInfo registers a client with peer ID and peer address
func (s *ProxyServer) RegisterClientByNameWithPeerInfo(name, ip, backendAddr string, conn net.Conn, peerID, peerAddr string) {
	// If conn is nil but IP is not "local", try to find existing connection from that IP
	// This handles SYNC case where services are registered without a direct connection
	if conn == nil && ip != "local" && ip != "" {
		for _, existing := range s.services {
			if existing != nil && existing.IP == ip && existing.Conn != nil && existing.Connected {
				conn = existing.Conn
				logging.Logf("[registry] found existing connection for ip=%s (reusing for name=%q)", ip, name)
				break
			}
		}
	}

	// Reuse a shared mutex for the same underlying connection.
	// One client pod may register multiple names on the same conn, and we must serialize
	// FORWARD/READY/data over that single stream.
	var connMu *sync.Mutex
	if conn != nil {
		for _, existing := range s.clients {
			if existing != nil && existing.Conn == conn && existing.ConnMu != nil {
				connMu = existing.ConnMu
				break
			}
		}
	}
	if connMu == nil {
		connMu = &sync.Mutex{}
	}

	// Check if connection is actually valid
	connected := ip == "local"
	if conn != nil {
		// Verify connection is still valid by checking remote address
		// If RemoteAddr() returns nil, connection is closed
		if conn.RemoteAddr() != nil {
			connected = true
		} else {
			connected = false
			logging.Logf("[registry] connection is closed for name=%q ip=%s peer_id=%q", name, ip, peerID)
		}
	}
	// Set PeerID: use provided peerID if available, otherwise fall back to defaults
	if peerID == "" {
		// Set PeerID: for local services, use current peer_id; for remote services, use IP as fallback
		peerID = ip
		if ip == "local" {
			peerID = logging.GetPeerID()
		}
	}

	// Determine the identifier to use for serviceKey: prefer peerID over IP
	// This ensures services are identified by peer_id (e.g., ops-proxy-xxx) rather than IP (e.g., 120.92.88.191)
	serviceIdentifier := peerID
	if serviceIdentifier == "" {
		serviceIdentifier = ip
	}

	// Remove all existing services with the same name to ensure name uniqueness
	// This ensures that if multiple peers register the same service name, only the latest one is kept
	// IMPORTANT: Only remove if the connection is the same or if it's a different peer
	// If it's the same connection, we should update in place rather than remove and re-add
	// First, collect all keys to delete (to avoid modifying map during iteration)
	keysToDelete := make([]string, 0)
	if existingClient, exists := s.clients[name]; exists {
		// Check if this is the same connection being reused
		// If so, we should update in place rather than remove
		if existingClient.Conn == conn && conn != nil {
			// Same connection, check if peer_id and other key info are the same
			// If peer_id is the same and other info hasn't changed, still update LastSeen and Connected
			existingPeerID := existingClient.PeerID
			if existingPeerID == "" {
				existingPeerID = existingClient.IP
			}
			newPeerID := peerID
			if newPeerID == "" {
				newPeerID = ip
			}

			// If peer_id is the same and backend/peerAddr haven't changed, just update LastSeen and Connected
			// This ensures connection state is kept fresh on heartbeat
			if existingPeerID == newPeerID &&
				existingClient.BackendAddr == backendAddr &&
				existingClient.PeerAddr == peerAddr {
				// Update LastSeen and Connected to keep connection state fresh
				existingClient.LastSeen = time.Now()
				existingClient.Connected = connected
				logging.Logf("[registry] heartbeat update - service unchanged name=%q peer_id=%q backend=%q connected=%t",
					name, existingPeerID, backendAddr, connected)
				return // Skip full update if nothing changed
			}

			// Same connection but info changed, update in place
			// This preserves the connection for findPeerAddrByConn
			logging.Logf("[registry] updating existing service with same name and connection name=%q old_ip=%s old_peer_id=%q new_peer_id=%q",
				name, existingClient.IP, existingClient.PeerID, peerID)
		} else {
			// Different connection or conn is nil, safe to remove old entry
			oldIdentifier := existingClient.PeerID
			if oldIdentifier == "" {
				oldIdentifier = existingClient.IP
			}
			oldKey := serviceKey(name, oldIdentifier)
			keysToDelete = append(keysToDelete, oldKey)
			logging.Logf("[registry] removing old service with same name name=%q old_ip=%s old_peer_id=%q (different connection)",
				name, existingClient.IP, existingClient.PeerID)
		}
	}
	// Also collect any other entries in services map with the same name (from other IPs/peerIDs)
	// But skip if they use the same connection (to avoid breaking active forwarding)
	for key, svc := range s.services {
		if svc != nil && svc.Name == name {
			// Skip if this is the same connection (will be updated in place)
			if svc.Conn == conn && conn != nil {
				continue
			}
			// Avoid duplicate keys
			found := false
			for _, k := range keysToDelete {
				if k == key {
					found = true
					break
				}
			}
			if !found {
				keysToDelete = append(keysToDelete, key)
				logging.Logf("[registry] removing duplicate service name=%q old_key=%s old_ip=%s old_peer_id=%q",
					name, key, svc.IP, svc.PeerID)
			}
		}
	}
	// Delete all collected keys
	for _, key := range keysToDelete {
		delete(s.services, key)
	}

	info := &types.ClientInfo{
		Name:        name,
		IP:          ip,
		BackendAddr: backendAddr,
		Conn:        conn,
		LastSeen:    time.Now(),
		// Local services are always "connected" (no connection needed, direct access)
		// Remote services need active connection
		Connected: connected,
		ConnMu:    connMu,
		PeerID:    peerID,
		PeerAddr:  peerAddr,
	}
	// Use name as key so we can route to the corresponding backend by name
	// This ensures name uniqueness: if multiple peers register the same name, only the latest one is kept
	s.clients[name] = info
	// Keep full list keyed by name+identifier (prefer peerID over IP for better identification)
	// This ensures services are identified by peer_id (e.g., ops-proxy-xxx) rather than IP (e.g., 120.92.88.191)
	s.services[serviceKey(name, serviceIdentifier)] = info

	connInfo := "nil"
	if conn != nil {
		if conn.RemoteAddr() != nil {
			connInfo = conn.RemoteAddr().String()
		} else {
			connInfo = "no-remote-addr"
		}
	}
	logging.Logf("[registry] registered name=%q ip=%s backend=%q peer_id=%q peer_addr=%q connected=%t conn=%v conn_info=%s", name, ip, backendAddr, peerID, peerAddr, connected, conn != nil, connInfo)

	// Update metrics
	if s.collector != nil {
		s.collector.RecordClientRegistration(name)
	}

	// Note: logServicesTable() is now only called in debug mode
	// If you need to print services table, use LogServicesTable() instead
}

// UnregisterClientByName unregisters a client by name
func (s *ProxyServer) UnregisterClientByName(name string) {

	if client, exists := s.clients[name]; exists {
		// Note: don't close connection, as other names might be using the same connection
		client.Connected = false
		logging.Logf("[registry] unregistered name=%q", name)
		delete(s.clients, name)
		for key, info := range s.services {
			if info != nil && info.Name == name {
				delete(s.services, key)
			}
		}

		// Update metrics
		if s.collector != nil {
			s.collector.RecordClientDisconnect(name)
		}
	}
}

// GetClient gets client information by name
// name is the name used during registration, used for routing
// For local services (IP="local"), returns even if Connected=false (no connection needed)
// For remote services, only returns if Connected=true (need active connection)
func (s *ProxyServer) GetClient(name string) *types.ClientInfo {

	logging.Logf("[GetClient] searching for name=%q services_count=%d", name, len(s.services))
	candidates := make([]*types.ClientInfo, 0)
	for key, client := range s.services {
		if client == nil {
			continue
		}
		if client.Name != name {
			continue
		}
		logging.Logf("[GetClient] found candidate key=%s name=%s ip=%s backend=%s connected=%t", key, client.Name, client.IP, client.BackendAddr, client.Connected)
		// Local services don't need connection, always include them
		if client.IP == "local" {
			candidates = append(candidates, client)
			logging.Logf("[GetClient] added local candidate name=%s", name)
		} else if client.Connected {
			// Remote services need active connection
			candidates = append(candidates, client)
			logging.Logf("[GetClient] added remote candidate name=%s ip=%s", name, client.IP)
		} else {
			logging.Logf("[GetClient] skipped disconnected remote service name=%s ip=%s", name, client.IP)
		}
	}
	if len(candidates) == 0 {
		logging.Logf("[GetClient] no candidates found for name=%q", name)
		return nil
	}
	// Prefer local services
	for _, client := range candidates {
		if client.IP == "local" {
			logging.Logf("[GetClient] returning local service name=%s backend=%s", name, client.BackendAddr)
			return client
		}
	}
	// If no local service, return first remote service
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].IP < candidates[j].IP
	})
	logging.Logf("[GetClient] returning remote service name=%s ip=%s backend=%s", name, candidates[0].IP, candidates[0].BackendAddr)
	return candidates[0]
}

// GetClientByPeerID finds a client connection by peer ID
// Returns the first connected client from the specified peer ID
func (s *ProxyServer) GetClientByPeerID(peerID string) *types.ClientInfo {
	if peerID == "" {
		return nil
	}
	
	logging.Logf("[GetClientByPeerID] searching for peer_id=%q services_count=%d", peerID, len(s.services))
	
	// First, try to find from peerServices map
	for peerIP, peerSvc := range s.peerServices {
		if peerSvc != nil && peerSvc.PeerID == peerID {
			if peerSvc.Conn != nil {
				// Check if connection is still valid
				if peerSvc.Conn.RemoteAddr() == nil {
					logging.Logf("[GetClientByPeerID] peer connection is closed peer_id=%s peer_ip=%s", peerID, peerIP)
					continue
				}
				// Find any service from this peer to get connection info
				for _, svc := range peerSvc.Services {
					if svc != nil {
						// Create a ClientInfo from peer service
						// IMPORTANT: This connection is the incoming connection from the peer
						// (e.g., Peer A connects to Peer B, so from Peer B's perspective, this is an incoming connection)
						// TCP connections are bidirectional, so Peer B can use this connection to send FORWARD to Peer A
						client := &types.ClientInfo{
							Name:        svc.Name,
							IP:          peerIP,
							BackendAddr: svc.BackendAddr,
							PeerID:      peerID,
							PeerAddr:    peerSvc.PeerAddr,
							Conn:        peerSvc.Conn,
							Connected:   true,
							LastSeen:    time.Now(),
						}
						connRemote := ""
						if peerSvc.Conn.RemoteAddr() != nil {
							connRemote = peerSvc.Conn.RemoteAddr().String()
						}
						logging.Logf("[GetClientByPeerID] found from peerServices peer_id=%s name=%s backend=%s conn_remote=%s", peerID, svc.Name, svc.BackendAddr, connRemote)
						return client
					}
				}
			} else {
				logging.Logf("[GetClientByPeerID] peer has no connection peer_id=%s peer_ip=%s", peerID, peerIP)
			}
		}
	}
	
	// Second, try to find from services map
	for key, client := range s.services {
		if client == nil {
			continue
		}
		clientPeerID := client.PeerID
		if clientPeerID == "" {
			clientPeerID = client.IP
		}
		if clientPeerID == peerID && client.Connected && client.Conn != nil {
			logging.Logf("[GetClientByPeerID] found from services key=%s peer_id=%s name=%s backend=%s", key, peerID, client.Name, client.BackendAddr)
			return client
		}
	}
	
	logging.Logf("[GetClientByPeerID] no client found for peer_id=%q", peerID)
	return nil
}

// GetClients gets all clients (for metrics collection)
// The returned map key is the registered name
func (s *ProxyServer) GetClients() map[string]*types.ClientInfo {

	result := make(map[string]*types.ClientInfo)
	for name, client := range s.clients {
		// Return all registered names; connected status is provided via IsClientConnected.
		result[name] = client
	}
	return result
}

// IsClientConnected checks if a client is connected
func (s *ProxyServer) IsClientConnected(name string) bool {

	for _, client := range s.services {
		if client != nil && client.Name == name && client.Connected {
			return true
		}
	}
	return false
}

// UnregisterClientsByConn removes all registered names bound to the given connection.
func (s *ProxyServer) UnregisterClientsByConn(conn net.Conn) {

	for name, client := range s.clients {
		if client != nil && client.Conn == conn {
			client.Connected = false
			delete(s.clients, name)
			if s.collector != nil {
				s.collector.RecordClientDisconnect(name)
			}
			logging.Logf("[registry] unregistered name=%q reason=conn_closed", name)
		}
	}
	for key, client := range s.services {
		if client != nil && client.Conn == conn {
			delete(s.services, key)
		}
	}
}

// servicesTableLines returns a stable table snapshot of current services map on this peer.
// Prints the full map structure with all entries.
func (s *ProxyServer) servicesTableLines() []string {
	if len(s.services) == 0 {
		return []string{"services=[]"}
	}

	type svcEntry struct {
		key     string
		name    string
		backend string
		peerID  string
	}
	entries := make([]svcEntry, 0, len(s.services))
	currentPeerID := logging.GetPeerID()
	for key, c := range s.services {
		if c == nil {
			continue
		}
		// Use PeerID if set, otherwise use IP (for backward compatibility)
		// For local services, use current peer_id
		peerID := c.PeerID
		if peerID == "" {
			if c.IP == "local" {
				peerID = currentPeerID
			} else {
				peerID = c.IP
			}
		}
		entries = append(entries, svcEntry{
			key:     key,
			name:    c.Name,
			backend: c.BackendAddr,
			peerID:  peerID,
		})
	}
	// Sort by key for consistent output
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })

	// Format as services=[name@peer_id,name@peer_id,...] similar to route details
	serviceItems := make([]string, 0, len(entries))
	for _, e := range entries {
		serviceItems = append(serviceItems, fmt.Sprintf("%s@%s", e.name, e.peerID))
	}
	// Sort for consistent output
	sort.Strings(serviceItems)
	servicesList := strings.Join(serviceItems, ",")
	lines := []string{fmt.Sprintf("services=[%s]", servicesList)}
	return lines
}

// logServicesTable prints the full services list in route details format.
// Only prints in debug mode if cfg is provided.
func (s *ProxyServer) logServicesTable(cfg *config.Config) {
	// Only print in debug mode
	if cfg == nil || cfg.Log.Level != "debug" {
		return
	}
	peerID := logging.GetPeerID()
	lines := s.servicesTableLines()
	if len(lines) == 0 {
		return
	}
	// Format as single line similar to route details
	servicesStr := strings.Join(lines, "")
	logging.Logf("[registry] services peer_id=%s %s", peerID, servicesStr)
}

// LogServicesTable prints the full services list in route details format (public wrapper).
// Always prints regardless of log level.
func (s *ProxyServer) LogServicesTable() {
	peerID := logging.GetPeerID()
	lines := s.servicesTableLines()
	if len(lines) == 0 {
		return
	}
	// Format as single line similar to route details
	servicesStr := strings.Join(lines, "")
	logging.Logf("[registry] services peer_id=%s %s", peerID, servicesStr)
}

// logPeerServicesMap prints the full peerServices map structure
func (s *ProxyServer) logPeerServicesMap() {

	peerID := logging.GetPeerID()

	if len(s.peerServices) == 0 {
		logging.Logf("[registry] peer_services_map peer_id=%s {}", peerID)
		return
	}

	logging.Logf("[registry] peer_services_map peer_id=%s {total_peers=%d}", peerID, len(s.peerServices))

	for peerIP, peerSvc := range s.peerServices {
		if peerSvc == nil {
			continue
		}
		// peerIP is the remote_peer_addr (connection address)
		// Extract peer_id from first service if available
		var remotePeerID string
		if len(peerSvc.Services) > 0 {
			for _, svc := range peerSvc.Services {
				if svc != nil && svc.SourcePeer != "" {
					// SourcePeer might be peer_id or IP, use what we have
					remotePeerID = svc.SourcePeer
					break
				}
			}
		}
		if remotePeerID == "" {
			remotePeerID = peerIP
		}
		logging.Logf("[registry] peer_services_map peer_id=%s   remote_peer_id=%s remote_peer_addr=%s {services=%d last_sync=%d}",
			peerID, remotePeerID, peerIP, len(peerSvc.Services), peerSvc.LastSync)

		if len(peerSvc.Services) > 0 {
			logging.Logf("[registry] peer_services_map peer_id=%s     | name | backend |", peerID)
			logging.Logf("[registry] peer_services_map peer_id=%s     | ---- | ------- |", peerID)
			for name, svc := range peerSvc.Services {
				if svc != nil {
					logging.Logf("[registry] peer_services_map peer_id=%s     | %s | %s |",
						peerID, name, svc.BackendAddr)
				}
			}
		}
	}
}

func serviceKey(name, source string) string {
	// Note: source here is IP for backward compatibility
	// In the future, we should use PeerID instead of IP for better identification
	return strings.TrimSpace(name) + "@" + strings.TrimSpace(source)
}

// servicesDebugSnapshot returns a stable snapshot of current services for routing logs.
// Format: name@peer, sorted by name.
func (s *ProxyServer) servicesDebugSnapshot() string {

	if len(s.services) == 0 {
		return "<empty>"
	}

	items := make([]string, 0, len(s.services))
	for _, c := range s.services {
		if c == nil {
			continue
		}
		// Use PeerID for better identification (not IP which might be proxy)
		peerIdentifier := c.PeerID
		if peerIdentifier == "" {
			peerIdentifier = c.IP
		}
		items = append(items, c.Name+"@"+peerIdentifier)
	}
	sort.Strings(items)
	return strings.Join(items, ",")
}

// servicesByNameSnapshot returns service entries for a given name.
// Format: peer_id=... backend=...
func (s *ProxyServer) servicesByNameSnapshot(name string) string {
	logging.Logf("[servicesByNameSnapshot] START name=%q", name)

	logging.Logf("[servicesByNameSnapshot] services count=%d", len(s.services))
	if strings.TrimSpace(name) == "" || len(s.services) == 0 {
		logging.Logf("[servicesByNameSnapshot] empty name or no services, returning <empty>")
		return "<empty>"
	}

	items := make([]string, 0)
	for key, c := range s.services {
		if c == nil {
			logging.Logf("[servicesByNameSnapshot] skipping nil client key=%q", key)
			continue
		}
		if c.Name != name {
			continue
		}
		logging.Logf("[servicesByNameSnapshot] found match key=%q name=%q peer_id=%q remote_peer_addr=%q backend=%q",
			key, c.Name, c.PeerID, c.IP, c.BackendAddr)
		// Use PeerID for display (not IP which might be proxy address)
		peerIdentifier := c.PeerID
		if peerIdentifier == "" {
			peerIdentifier = c.IP
		}
		items = append(items, "peer_id="+peerIdentifier+" backend="+c.BackendAddr)
	}
	if len(items) == 0 {
		logging.Logf("[servicesByNameSnapshot] no matches found, returning <empty>")
		return "<empty>"
	}
	sort.Strings(items)
	result := strings.Join(items, "; ")
	logging.Logf("[servicesByNameSnapshot] returning result=%q", result)
	return result
}

// RegisterLocalServices registers SERVICE_ADDR entries into the local peer registry.
func (s *ProxyServer) RegisterLocalServices(serviceAddr string, defaultName string) int {
	if strings.TrimSpace(serviceAddr) == "" {
		return 0
	}
	if defaultName == "" {
		defaultName = "local"
	}
	parsed := routing.ParseBackendAddrString(serviceAddr, defaultName)
	logging.Logf("[registry] parsing SERVICE_ADDR: %q -> %d service(s)", serviceAddr, len(parsed))
	registered := 0
	for i, b := range parsed {
		logging.Logf("[registry] processing service %d/%d: name=%q address=%q", i+1, len(parsed), b.Name, b.Address)

		// Validate service name and address
		if strings.TrimSpace(b.Name) == "" || strings.TrimSpace(b.Address) == "" {
			logging.Logf("[registry] skipping invalid service: name=%q address=%q", b.Name, b.Address)
			continue
		}

		// Check if name looks like a port number (invalid service name)
		if _, err := strconv.Atoi(b.Name); err == nil {
			logging.Logf("[registry] skipping service with numeric name (likely misconfigured): name=%q address=%q", b.Name, b.Address)
			continue
		}

		logging.Logf("[registry] registering local service: name=%q backend=%q", b.Name, b.Address)
		s.RegisterClientByName(b.Name, "local", b.Address, nil)
		registered++
		logging.Logf("[registry] registered service %d/%d: name=%q backend=%q", i+1, len(parsed), b.Name, b.Address)
	}
	// Note: Service table is only printed in debug mode via logServicesTable(cfg)
	// RegisterLocalServices doesn't have cfg parameter, so we skip printing here
	// If you need to print services table, use LogServicesTable() instead
	return registered
}

// GetAllServicesExcept returns all services except those from the specified source IP.
// Used to sync service list to newly registered peers.
// Design: Only sync LOCAL services, not services learned from other peers
// This ensures Peer B and Peer C don't see each other's services (only see Peer A's local services)
func (s *ProxyServer) GetAllServicesExcept(excludeIP string) []struct {
	Name    string
	Backend string
} {

	result := make([]struct {
		Name    string
		Backend string
	}, 0)
	seen := make(map[string]bool) // name:backend to avoid duplicates

	for _, c := range s.services {
		if c == nil || !c.Connected {
			continue
		}
		// Design requirement: Only sync LOCAL services
		// Peer B and Peer C should not see each other's services
		// Only sync services with IP="local" (this peer's own services)
		if c.IP != "local" {
			continue
		}
		// Exclude services from the specified IP (should not happen for local services)
		if c.IP == excludeIP {
			continue
		}
		// Avoid duplicates (same name:backend from different sources)
		key := c.Name + ":" + c.BackendAddr
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, struct {
			Name    string
			Backend string
		}{
			Name:    c.Name,
			Backend: c.BackendAddr,
		})
	}
	return result
}
