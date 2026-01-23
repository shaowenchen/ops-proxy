package server

import (
	"net"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ops-proxy/pkg/config"
	"github.com/ops-proxy/pkg/logging"
	"github.com/ops-proxy/pkg/metrics"
	"github.com/ops-proxy/pkg/types"
)

// NewProxyServer creates a new proxy server
func NewProxyServer(cfg *config.Config) (*ProxyServer, error) {
	registry := prometheus.NewRegistry()
	
	server := &ProxyServer{
		clients:  make(map[string]*types.ClientInfo),
		services: make(map[string]*types.ClientInfo),
		peerServices: make(map[string]*types.PeerServices),
		pendingData: make(map[string]chan net.Conn),
		registry: registry,
	}
	
	// Create collector with callbacks that use this server instance
	collector := metrics.NewCollector(
		func() map[string]interface{} {
			clients := make(map[string]interface{})
			allClients := server.GetClients()
			for k, v := range allClients {
				clients[k] = v
			}
			return clients
		},
		func(name string) bool {
			return server.IsClientConnected(name)
		},
	)
	
	server.collector = collector
	registry.MustRegister(collector)
	
	return server, nil
}

// StartMetricsServer starts the metrics server
func (s *ProxyServer) StartMetricsServer(metricsAddr, metricsPath string) error {
	http.Handle(metricsPath, promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
<head><title>Ops Proxy Exporter</title></head>
<body>
<h1>Ops Proxy Exporter</h1>
<p><a href="` + metricsPath + `">Metrics</a></p>
</body>
</html>`))
	})

	logging.Logf("[listen] metrics addr=%s path=%s health=/healthz", metricsAddr, metricsPath)
	return http.ListenAndServe(metricsAddr, nil)
}
