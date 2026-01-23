package mesh

import (
	"net"
	"time"

	"github.com/ops-proxy/pkg/config"
	"github.com/ops-proxy/pkg/logging"
)

// LinkInfo contains connection metadata for a peer link.
type LinkInfo struct {
	Addr        string
	Conn        net.Conn
	RemoteAddr  string
	ConnectedAt time.Time
}

// RunPeerLink maintains a long-lived connection to a peer and provides
// connection info to the caller for service sync and forwarding.
func RunPeerLink(addr string, cfg *config.Config, handler func(LinkInfo) error) error {
	reconnectInterval := 5 * time.Second
	maxReconnect := 0
	if cfg != nil {
		reconnectInterval = cfg.GetReconnectInterval()
		maxReconnect = cfg.Peer.MaxReconnect
	}

	reconnectCount := 0
	for {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			logging.Logf("Failed to connect to peer %s: %v", addr, err)
			if maxReconnect > 0 && reconnectCount >= maxReconnect {
				return err
			}
			reconnectCount++
			logging.Logf("Reconnecting to %s in %v (attempt %d)...", addr, reconnectInterval, reconnectCount)
			time.Sleep(reconnectInterval)
			continue
		}

		reconnectCount = 0
		link := LinkInfo{
			Addr:        addr,
			Conn:        conn,
			RemoteAddr:  safeRemoteAddr(conn),
			ConnectedAt: time.Now(),
		}

		err = handler(link)
		_ = conn.Close()
		if err != nil {
			logging.Logf("Connection to %s closed: %v", addr, err)
		}

		if maxReconnect > 0 && reconnectCount >= maxReconnect {
			return err
		}
		reconnectCount++
		logging.Logf("Reconnecting to %s in %v (attempt %d)...", addr, reconnectInterval, reconnectCount)
		time.Sleep(reconnectInterval)
	}
}

func safeRemoteAddr(conn net.Conn) string {
	if conn == nil || conn.RemoteAddr() == nil {
		return ""
	}
	return conn.RemoteAddr().String()
}
