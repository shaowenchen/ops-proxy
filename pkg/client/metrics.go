package client

import (
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// metricsCollector exports client-side forwarding metrics.
// This is separate from server-side ops_proxy_* metrics.
type metricsCollector struct {
	info              *prometheus.Desc
	forwardsTotal       *prometheus.Desc
	forwardsFailedTotal *prometheus.Desc
	activeForwards      *prometheus.Desc

	// state
	mu            sync.RWMutex
	forwards      map[string]float64            // client_name -> count
	forwardsFail  map[string]map[string]float64 // client_name -> reason -> count
	active        map[string]float64            // client_name -> gauge
}

var (
	clientMetricsOnce sync.Once
	clientMetrics     *metricsCollector
)

// NewMetricsCollector returns a singleton prometheus.Collector for client-side metrics.
func NewMetricsCollector() prometheus.Collector {
	clientMetricsOnce.Do(func() {
		clientMetrics = &metricsCollector{
			info: prometheus.NewDesc(
				"ops_proxy_client_info",
				"Client process info metric (always 1)",
				[]string{"node", "pod"},
				nil,
			),
			forwardsTotal: prometheus.NewDesc(
				"ops_proxy_client_forwards_total",
				"Total number of forward requests handled by client (by routing name)",
				[]string{"client_name", "node", "pod"},
				nil,
			),
			forwardsFailedTotal: prometheus.NewDesc(
				"ops_proxy_client_forwards_failed_total",
				"Total number of failed forward requests handled by client (by routing name and reason)",
				[]string{"client_name", "reason", "node", "pod"},
				nil,
			),
			activeForwards: prometheus.NewDesc(
				"ops_proxy_client_active_forwards",
				"Current number of in-flight forward requests on client (by routing name)",
				[]string{"client_name", "node", "pod"},
				nil,
			),
			forwards:     make(map[string]float64),
			forwardsFail: make(map[string]map[string]float64),
			active:       make(map[string]float64),
		}
	})
	return clientMetrics
}

func (m *metricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.info
	ch <- m.forwardsTotal
	ch <- m.forwardsFailedTotal
	ch <- m.activeForwards
}

func (m *metricsCollector) Collect(ch chan<- prometheus.Metric) {
	node := os.Getenv("NODE_NAME")
	if node == "" {
		node = "unknown"
	}
	pod := os.Getenv("POD_NAME")
	if pod == "" {
		pod = os.Getenv("HOSTNAME")
		if pod == "" {
			pod = "unknown"
		}
	}

	ch <- prometheus.MustNewConstMetric(m.info, prometheus.GaugeValue, 1, node, pod)

	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, v := range m.forwards {
		ch <- prometheus.MustNewConstMetric(m.forwardsTotal, prometheus.CounterValue, v, name, node, pod)
	}
	for name, byReason := range m.forwardsFail {
		for reason, v := range byReason {
			ch <- prometheus.MustNewConstMetric(m.forwardsFailedTotal, prometheus.CounterValue, v, name, reason, node, pod)
		}
	}
	for name, v := range m.active {
		ch <- prometheus.MustNewConstMetric(m.activeForwards, prometheus.GaugeValue, v, name, node, pod)
	}
}

func recordClientForwardStart(name string) {
	if clientMetrics == nil {
		return
	}
	clientMetrics.mu.Lock()
	defer clientMetrics.mu.Unlock()
	clientMetrics.active[name]++
}

func recordClientForwardSuccess(name string) {
	if clientMetrics == nil {
		return
	}
	clientMetrics.mu.Lock()
	defer clientMetrics.mu.Unlock()
	clientMetrics.forwards[name]++
	if clientMetrics.active[name] > 0 {
		clientMetrics.active[name]--
	}
}

func recordClientForwardFail(name, reason string) {
	if clientMetrics == nil {
		return
	}
	clientMetrics.mu.Lock()
	defer clientMetrics.mu.Unlock()
	clientMetrics.forwards[name]++
	if _, ok := clientMetrics.forwardsFail[name]; !ok {
		clientMetrics.forwardsFail[name] = make(map[string]float64)
	}
	clientMetrics.forwardsFail[name][reason]++
	if clientMetrics.active[name] > 0 {
		clientMetrics.active[name]--
	}
}

