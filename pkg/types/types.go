package types

import (
	"net"
	"sync"
	"time"
)

// ClientInfo client information
type ClientInfo struct {
	Name        string
	IP          string
	BackendAddr string
	Conn        net.Conn
	LastSeen    time.Time
	Connected   bool
	ConnMu      *sync.Mutex // Shared mutex for the underlying Conn (multiple names can share one Conn)
	PeerID      string      // Peer ID for display (local peer_id for local services, remote peer_id or IP for remote services)
}

