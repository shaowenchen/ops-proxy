package peer

import (
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ops-proxy/pkg/logging"
	"github.com/ops-proxy/pkg/metrics"
	"github.com/ops-proxy/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ProxyPeer represents a peer node that can both listen and connect to other peers
type ProxyPeer struct {
	// Registered peers (services registered by other peers)
	peers     map[string]*types.ClientInfo
	peersLock sync.RWMutex
	registry  *prometheus.Registry
	collector *metrics.Collector

	// pendingData is used to pair a proxy request with the next DATA connection from peer.
	// Keyed by proxy-id. Supports concurrency per name.
	pendingData map[string]chan net.Conn
	pendingLock sync.Mutex
	nextProxyID atomic.Uint64

	// accept EOF log throttling (to avoid flooding debug logs)
	acceptEOFLock       sync.Mutex
	acceptEOFLastLogAt  time.Time
	acceptEOFSuppressed int
}

// NewProxyPeer creates a new proxy peer
func NewProxyPeer() (*ProxyPeer, error) {
	registry := prometheus.NewRegistry()

	peer := &ProxyPeer{
		peers:       make(map[string]*types.ClientInfo),
		pendingData: make(map[string]chan net.Conn),
		registry:    registry,
	}

	// Create collector with callbacks that use this peer instance
	collector := metrics.NewCollector(
		func() map[string]interface{} {
			peers := make(map[string]interface{})
			allPeers := peer.GetPeers()
			for k, v := range allPeers {
				peers[k] = v
			}
			return peers
		},
		func(name string) bool {
			return peer.IsPeerConnected(name)
		},
	)

	peer.collector = collector
	registry.MustRegister(collector)

	return peer, nil
}

// StartMetricsServer starts the metrics server
func (p *ProxyPeer) StartMetricsServer(metricsAddr, metricsPath string) error {
	http.Handle(metricsPath, promhttp.HandlerFor(p.registry, promhttp.HandlerOpts{}))
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
<head><title>Ops Proxy Peer Exporter</title></head>
<body>
<h1>Ops Proxy Peer Exporter</h1>
<p><a href="` + metricsPath + `">Metrics</a></p>
</body>
</html>`))
	})

	logging.Logf("[listen] metrics addr=%s path=%s", metricsAddr, metricsPath)
	return http.ListenAndServe(metricsAddr, nil)
}

// GetPeers gets all registered peers (for metrics collection)
func (p *ProxyPeer) GetPeers() map[string]*types.ClientInfo {
	p.peersLock.RLock()
	defer p.peersLock.RUnlock()
	result := make(map[string]*types.ClientInfo)
	for k, v := range p.peers {
		result[k] = v
	}
	return result
}

// IsPeerConnected checks if a peer is connected
func (p *ProxyPeer) IsPeerConnected(name string) bool {
	p.peersLock.RLock()
	defer p.peersLock.RUnlock()
	if peer, exists := p.peers[name]; exists {
		return peer.Connected
	}
	return false
}
