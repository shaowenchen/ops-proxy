package metrics

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Collector Prometheus metrics collector
type Collector struct {
	GetClients      func() map[string]interface{}
	GetClientStatus func(string) bool

	// Info metric (always 1)
	serverInfo *prometheus.Desc

	// Client metrics
	clientsTotal             *prometheus.Desc
	clientsConnected         *prometheus.Desc
	clientUp                 *prometheus.Desc
	clientRegistrationsTotal *prometheus.Desc
	clientDisconnectsTotal   *prometheus.Desc

	// Connection metrics
	connectionsTotal   *prometheus.Desc
	connectionsActive  *prometheus.Desc
	connectionsFailed  *prometheus.Desc
	connectionsBytesTx *prometheus.Desc
	connectionsBytesRx *prometheus.Desc

	// Proxy metrics
	proxyRequestsTotal   *prometheus.Desc
	proxyRequestsSuccess *prometheus.Desc
	proxyRequestsFailed  *prometheus.Desc
	proxyLatencySeconds  *prometheus.Desc

	// Tunnel metrics (proxy-id)
	forwardCommandsTotal      *prometheus.Desc
	dataConnectionsMatched    *prometheus.Desc
	dataConnectionsTimeout    *prometheus.Desc
	dataConnectionsUnexpected *prometheus.Desc
	pendingDataConnections    *prometheus.Desc

	// Reverse proxy metrics removed (now handled via SERVICE_ADDR local services)

	// Error metrics (low cardinality)
	proxyErrorsTotal *prometheus.Desc

	// Metrics counters (protected by mutex)
	metricsLock            sync.RWMutex
	clientRegistrations    map[string]float64
	clientDisconnects      map[string]float64
	connectionsCount       map[string]float64
	connectionsFailedCount map[string]float64
	connectionsBytesTxMap  map[string]float64
	connectionsBytesRxMap  map[string]float64
	requestsCount          map[string]float64
	requestsSuccessCount   map[string]float64
	requestsFailedCount    map[string]float64
	latencySum             map[string]float64
	latencyCount           map[string]float64

	// Tunnel counters (proxy-id)
	forwardCommandsByName   map[string]float64
	dataMatchedByName       map[string]float64
	dataTimeoutByName       map[string]float64
	dataUnexpectedTotal     float64
	pendingDataConnectionsN float64

	// Active/inflight by name
	connectionsActiveByName map[string]float64
	// Error counters keyed by "client_name:reason"
	proxyErrorsByKey map[string]float64
}

// NewCollector creates a new metrics collector
func NewCollector(getClients func() map[string]interface{}, getClientStatus func(string) bool) *Collector {
	return &Collector{
		GetClients:      getClients,
		GetClientStatus: getClientStatus,
		serverInfo: prometheus.NewDesc(
			"ops_proxy_server_info",
			"Peer listening process info metric (always 1). Present when peer is listening for connections.",
			[]string{"node", "pod"},
			nil,
		),
		clientsTotal: prometheus.NewDesc(
			"ops_proxy_clients_total",
			"Total number of registered routing names (services) in this listening peer instance",
			[]string{"node", "pod"},
			nil,
		),
		clientsConnected: prometheus.NewDesc(
			"ops_proxy_clients_connected",
			"Number of currently connected routing names (services) in this listening peer instance",
			[]string{"node", "pod"},
			nil,
		),
		clientUp: prometheus.NewDesc(
			"ops_proxy_client_up",
			"Service connection status by registered name (1=connected, 0=disconnected)",
			[]string{"client_name", "node", "pod"},
			nil,
		),
		clientRegistrationsTotal: prometheus.NewDesc(
			"ops_proxy_client_registrations_total",
			"Total REGISTER refreshes received for a routing name (service)",
			[]string{"client_name", "node", "pod"},
			nil,
		),
		clientDisconnectsTotal: prometheus.NewDesc(
			"ops_proxy_client_disconnects_total",
			"Total disconnect/unregister events for a routing name (service)",
			[]string{"client_name", "node", "pod"},
			nil,
		),
		connectionsTotal: prometheus.NewDesc(
			"ops_proxy_connections_total",
			"Total number of proxy connections",
			[]string{"client_name", "node", "pod"},
			nil,
		),
		connectionsActive: prometheus.NewDesc(
			"ops_proxy_connections_active",
			"Number of active proxy connections",
			[]string{"client_name", "node", "pod"},
			nil,
		),
		connectionsFailed: prometheus.NewDesc(
			"ops_proxy_connections_failed_total",
			"Total number of failed proxy connections",
			[]string{"client_name", "node", "pod"},
			nil,
		),
		connectionsBytesTx: prometheus.NewDesc(
			"ops_proxy_connections_bytes_tx_total",
			"Total bytes transmitted",
			[]string{"client_name", "node", "pod"},
			nil,
		),
		connectionsBytesRx: prometheus.NewDesc(
			"ops_proxy_connections_bytes_rx_total",
			"Total bytes received",
			[]string{"client_name", "node", "pod"},
			nil,
		),
		proxyRequestsTotal: prometheus.NewDesc(
			"ops_proxy_requests_total",
			"Total number of proxy requests",
			[]string{"client_name", "protocol", "node", "pod"},
			nil,
		),
		proxyRequestsSuccess: prometheus.NewDesc(
			"ops_proxy_requests_success_total",
			"Total number of successful proxy requests",
			[]string{"client_name", "protocol", "node", "pod"},
			nil,
		),
		proxyRequestsFailed: prometheus.NewDesc(
			"ops_proxy_requests_failed_total",
			"Total number of failed proxy requests",
			[]string{"client_name", "protocol", "node", "pod"},
			nil,
		),
		proxyLatencySeconds: prometheus.NewDesc(
			"ops_proxy_latency_seconds",
			"Proxy request latency in seconds",
			[]string{"client_name", "protocol", "node", "pod"},
			nil,
		),
		forwardCommandsTotal: prometheus.NewDesc(
			"ops_proxy_forward_commands_total",
			"Total number of FORWARD commands sent to clients",
			[]string{"client_name", "node", "pod"},
			nil,
		),
		dataConnectionsMatched: prometheus.NewDesc(
			"ops_proxy_data_connections_matched_total",
			"Total number of DATA connections successfully matched to a proxy-id",
			[]string{"client_name", "node", "pod"},
			nil,
		),
		dataConnectionsTimeout: prometheus.NewDesc(
			"ops_proxy_data_connections_timeout_total",
			"Total number of timeouts waiting for DATA connection (per proxy-id)",
			[]string{"client_name", "node", "pod"},
			nil,
		),
		dataConnectionsUnexpected: prometheus.NewDesc(
			"ops_proxy_data_connections_unexpected_total",
			"Total number of unexpected DATA connections (no pending proxy-id)",
			[]string{"node", "pod"},
			nil,
		),
		pendingDataConnections: prometheus.NewDesc(
			"ops_proxy_pending_data_connections",
			"Current number of pending proxy-ids waiting for DATA connection",
			[]string{"node", "pod"},
			nil,
		),
		proxyErrorsTotal: prometheus.NewDesc(
			"ops_proxy_proxy_errors_total",
			"Total number of proxy/tunnel errors by reason",
			[]string{"client_name", "reason", "node", "pod"},
			nil,
		),
		clientRegistrations:     make(map[string]float64),
		clientDisconnects:       make(map[string]float64),
		connectionsCount:        make(map[string]float64),
		connectionsFailedCount:  make(map[string]float64),
		connectionsBytesTxMap:   make(map[string]float64),
		connectionsBytesRxMap:   make(map[string]float64),
		requestsCount:           make(map[string]float64),
		requestsSuccessCount:    make(map[string]float64),
		requestsFailedCount:     make(map[string]float64),
		latencySum:              make(map[string]float64),
		latencyCount:            make(map[string]float64),
		forwardCommandsByName:   make(map[string]float64),
		dataMatchedByName:       make(map[string]float64),
		dataTimeoutByName:       make(map[string]float64),
		connectionsActiveByName: make(map[string]float64),
		proxyErrorsByKey:        make(map[string]float64),
	}
}

// IncActiveConnection increments active connections gauge for a client name.
func (c *Collector) IncActiveConnection(clientName string) {
	c.metricsLock.Lock()
	defer c.metricsLock.Unlock()
	c.connectionsActiveByName[clientName]++
}

// DecActiveConnection decrements active connections gauge for a client name.
func (c *Collector) DecActiveConnection(clientName string) {
	c.metricsLock.Lock()
	defer c.metricsLock.Unlock()
	if c.connectionsActiveByName[clientName] > 0 {
		c.connectionsActiveByName[clientName]--
	}
}

// RecordProxyError records a proxy/tunnel error by reason (low cardinality).
func (c *Collector) RecordProxyError(clientName, reason string) {
	c.metricsLock.Lock()
	defer c.metricsLock.Unlock()
	key := fmt.Sprintf("%s:%s", clientName, reason)
	c.proxyErrorsByKey[key]++
}

// UpdateConnectionMetrics updates connection metrics
func (c *Collector) UpdateConnectionMetrics(clientName, protocol string, success bool, bytesTx, bytesRx int64, duration time.Duration) {
	c.metricsLock.Lock()
	defer c.metricsLock.Unlock()

	key := clientName
	protocolKey := fmt.Sprintf("%s:%s", clientName, protocol)

	c.connectionsCount[key]++
	c.connectionsBytesTxMap[key] += float64(bytesTx)
	c.connectionsBytesRxMap[key] += float64(bytesRx)
	c.requestsCount[protocolKey]++

	if success {
		c.requestsSuccessCount[protocolKey]++
		c.latencySum[protocolKey] += duration.Seconds()
		c.latencyCount[protocolKey]++
	} else {
		c.connectionsFailedCount[key]++
		c.requestsFailedCount[protocolKey]++
	}
}

// RecordClientRegistration records client registration
func (c *Collector) RecordClientRegistration(clientName string) {
	c.metricsLock.Lock()
	defer c.metricsLock.Unlock()
	c.clientRegistrations[clientName]++
}

// RecordClientDisconnect records client disconnection
func (c *Collector) RecordClientDisconnect(clientName string) {
	c.metricsLock.Lock()
	defer c.metricsLock.Unlock()
	c.clientDisconnects[clientName]++
}

// RecordForwardCommand records a FORWARD command sent to a client (by registered name).
func (c *Collector) RecordForwardCommand(clientName string) {
	c.metricsLock.Lock()
	defer c.metricsLock.Unlock()
	c.forwardCommandsByName[clientName]++
}

// RecordDataMatched records a DATA connection matched to a proxy-id (by registered name).
func (c *Collector) RecordDataMatched(clientName string) {
	c.metricsLock.Lock()
	defer c.metricsLock.Unlock()
	c.dataMatchedByName[clientName]++
}

// RecordDataTimeout records a timeout waiting for DATA connection (by registered name).
func (c *Collector) RecordDataTimeout(clientName string) {
	c.metricsLock.Lock()
	defer c.metricsLock.Unlock()
	c.dataTimeoutByName[clientName]++
}

// RecordDataUnexpected records an unexpected DATA connection (no pending proxy-id).
func (c *Collector) RecordDataUnexpected() {
	c.metricsLock.Lock()
	defer c.metricsLock.Unlock()
	c.dataUnexpectedTotal++
}

// SetPendingDataConnections sets current pending proxy-id count.
func (c *Collector) SetPendingDataConnections(n int) {
	c.metricsLock.Lock()
	defer c.metricsLock.Unlock()
	c.pendingDataConnectionsN = float64(n)
}

// Describe implements prometheus.Collector interface
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.serverInfo
	ch <- c.clientsTotal
	ch <- c.clientsConnected
	ch <- c.clientUp
	ch <- c.clientRegistrationsTotal
	ch <- c.clientDisconnectsTotal
	ch <- c.connectionsTotal
	ch <- c.connectionsActive
	ch <- c.connectionsFailed
	ch <- c.connectionsBytesTx
	ch <- c.connectionsBytesRx
	ch <- c.proxyRequestsTotal
	ch <- c.proxyRequestsSuccess
	ch <- c.proxyRequestsFailed
	ch <- c.proxyLatencySeconds
	ch <- c.forwardCommandsTotal
	ch <- c.dataConnectionsMatched
	ch <- c.dataConnectionsTimeout
	ch <- c.dataConnectionsUnexpected
	ch <- c.pendingDataConnections
	ch <- c.proxyErrorsTotal
}

// Collect implements prometheus.Collector interface
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		nodeName = "unknown"
	}

	podName := os.Getenv("POD_NAME")
	if podName == "" {
		podName = os.Getenv("HOSTNAME")
		if podName == "" {
			podName = "unknown"
		}
	}

	ch <- prometheus.MustNewConstMetric(
		c.serverInfo,
		prometheus.GaugeValue,
		1,
		nodeName, podName,
	)

	clients := c.GetClients()
	totalClients := len(clients)
	connectedClients := 0
	for name := range clients {
		if c.GetClientStatus(name) {
			connectedClients++
		}
	}

	ch <- prometheus.MustNewConstMetric(
		c.clientsTotal,
		prometheus.GaugeValue,
		float64(totalClients),
		nodeName, podName,
	)

	ch <- prometheus.MustNewConstMetric(
		c.clientsConnected,
		prometheus.GaugeValue,
		float64(connectedClients),
		nodeName, podName,
	)

	// Per-name connection status (gauge)
	for name := range clients {
		v := 0.0
		if c.GetClientStatus(name) {
			v = 1.0
		}
		ch <- prometheus.MustNewConstMetric(
			c.clientUp,
			prometheus.GaugeValue,
			v,
			name, nodeName, podName,
		)
	}

	// Collect metrics from counters
	c.metricsLock.RLock()
	defer c.metricsLock.RUnlock()

	for name, value := range c.clientRegistrations {
		ch <- prometheus.MustNewConstMetric(
			c.clientRegistrationsTotal,
			prometheus.CounterValue,
			value,
			name, nodeName, podName,
		)
	}

	for name, value := range c.clientDisconnects {
		ch <- prometheus.MustNewConstMetric(
			c.clientDisconnectsTotal,
			prometheus.CounterValue,
			value,
			name, nodeName, podName,
		)
	}

	for name, value := range c.connectionsCount {
		ch <- prometheus.MustNewConstMetric(
			c.connectionsTotal,
			prometheus.CounterValue,
			value,
			name, nodeName, podName,
		)
	}

	// Active connections (gauge) by name
	for name, value := range c.connectionsActiveByName {
		ch <- prometheus.MustNewConstMetric(
			c.connectionsActive,
			prometheus.GaugeValue,
			value,
			name, nodeName, podName,
		)
	}

	for name, value := range c.connectionsFailedCount {
		ch <- prometheus.MustNewConstMetric(
			c.connectionsFailed,
			prometheus.CounterValue,
			value,
			name, nodeName, podName,
		)
	}

	for name, value := range c.connectionsBytesTxMap {
		ch <- prometheus.MustNewConstMetric(
			c.connectionsBytesTx,
			prometheus.CounterValue,
			value,
			name, nodeName, podName,
		)
	}

	for name, value := range c.connectionsBytesRxMap {
		ch <- prometheus.MustNewConstMetric(
			c.connectionsBytesRx,
			prometheus.CounterValue,
			value,
			name, nodeName, podName,
		)
	}

	for key, value := range c.requestsCount {
		parts := strings.Split(key, ":")
		if len(parts) == 2 {
			ch <- prometheus.MustNewConstMetric(
				c.proxyRequestsTotal,
				prometheus.CounterValue,
				value,
				parts[0], parts[1], nodeName, podName,
			)
		}
	}

	for key, value := range c.requestsSuccessCount {
		parts := strings.Split(key, ":")
		if len(parts) == 2 {
			ch <- prometheus.MustNewConstMetric(
				c.proxyRequestsSuccess,
				prometheus.CounterValue,
				value,
				parts[0], parts[1], nodeName, podName,
			)
		}
	}

	for key, value := range c.requestsFailedCount {
		parts := strings.Split(key, ":")
		if len(parts) == 2 {
			ch <- prometheus.MustNewConstMetric(
				c.proxyRequestsFailed,
				prometheus.CounterValue,
				value,
				parts[0], parts[1], nodeName, podName,
			)
		}
	}

	for key, sum := range c.latencySum {
		parts := strings.Split(key, ":")
		if len(parts) == 2 && c.latencyCount[key] > 0 {
			avg := sum / c.latencyCount[key]
			ch <- prometheus.MustNewConstMetric(
				c.proxyLatencySeconds,
				prometheus.GaugeValue,
				avg,
				parts[0], parts[1], nodeName, podName,
			)
		}
	}

	// Tunnel metrics
	for name, value := range c.forwardCommandsByName {
		ch <- prometheus.MustNewConstMetric(
			c.forwardCommandsTotal,
			prometheus.CounterValue,
			value,
			name, nodeName, podName,
		)
	}
	for name, value := range c.dataMatchedByName {
		ch <- prometheus.MustNewConstMetric(
			c.dataConnectionsMatched,
			prometheus.CounterValue,
			value,
			name, nodeName, podName,
		)
	}
	for name, value := range c.dataTimeoutByName {
		ch <- prometheus.MustNewConstMetric(
			c.dataConnectionsTimeout,
			prometheus.CounterValue,
			value,
			name, nodeName, podName,
		)
	}
	ch <- prometheus.MustNewConstMetric(
		c.dataConnectionsUnexpected,
		prometheus.CounterValue,
		c.dataUnexpectedTotal,
		nodeName, podName,
	)
	ch <- prometheus.MustNewConstMetric(
		c.pendingDataConnections,
		prometheus.GaugeValue,
		c.pendingDataConnectionsN,
		nodeName, podName,
	)

	for key, value := range c.proxyErrorsByKey {
		parts := strings.Split(key, ":")
		if len(parts) == 2 {
			ch <- prometheus.MustNewConstMetric(
				c.proxyErrorsTotal,
				prometheus.CounterValue,
				value,
				parts[0], parts[1], nodeName, podName,
			)
		}
	}
}
