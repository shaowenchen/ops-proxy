package server

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ops-proxy/pkg/metrics"
	"github.com/ops-proxy/pkg/types"
)

// ProxyServer proxy server
type ProxyServer struct {
	clients     map[string]*types.ClientInfo
	services    map[string]*types.ClientInfo
	clientsLock sync.RWMutex
	registry    *prometheus.Registry
	collector   *metrics.Collector

	// peerServices stores services from remote peers
	// Key: peer IP address, Value: PeerServices struct
	peerServices map[string]*types.PeerServices
	peerServicesLock sync.RWMutex

	// pendingData is used to pair a proxy request with the next DATA connection from client.
	// Keyed by proxy-id. Supports concurrency per name.
	pendingData map[string]chan net.Conn
	pendingLock sync.Mutex
	nextProxyID atomic.Uint64

	// accept EOF log throttling (to avoid flooding debug logs)
	acceptEOFLock       sync.Mutex
	acceptEOFLastLogAt  time.Time
	acceptEOFSuppressed int
}

