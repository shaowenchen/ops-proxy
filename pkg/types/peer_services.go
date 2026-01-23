package types

// PeerServices stores services from a remote peer
type PeerServices struct {
	PeerIP   string                    // IP address of the peer
	Services map[string]*ServiceInfo   // Map of service name -> ServiceInfo
	LastSync int64                     // Timestamp of last sync
}

// ServiceInfo represents a service from a remote peer
type ServiceInfo struct {
	Name        string // Service name (domain)
	BackendAddr string // Backend address
	SourcePeer  string // Source peer IP
}
