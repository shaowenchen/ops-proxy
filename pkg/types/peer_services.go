package types

import "net"

// PeerServices stores services from a remote peer
type PeerServices struct {
	PeerIP   string                    // IP address of the peer (remote_peer_addr, connection address)
	PeerID   string                    // Peer ID for display (e.g., ops-proxy-xxx)
	PeerAddr string                    // Peer's real bind address (e.g., "10.0.1.2:6443") for DATA connections
	Conn     net.Conn                  // Control connection to this peer (for sending FORWARD commands)
	Services map[string]*ServiceInfo   // Map of service name -> ServiceInfo
	LastSync int64                     // Timestamp of last sync
}

// ServiceInfo represents a service from a remote peer
type ServiceInfo struct {
	Name        string // Service name (domain)
	BackendAddr string // Backend address
	SourcePeer  string // Source peer IP
}
