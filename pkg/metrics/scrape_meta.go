package metrics

import "os"

// ScrapingMeta returns labels embedded in ops_proxy_* time series. The Prometheus
// `cluster` label (and other target metadata) is expected to be attached at scrape
// time by the collector (e.g. GMP, Prometheus Operator).
//
// POD_NAMESPACE can be set via Kubernetes downward API so `namespace` is stable
// even when not using Kubernetes service discovery.
func ScrapingMeta() (namespace, node, pod string) {
	namespace = os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "unknown"
	}
	node = os.Getenv("NODE_NAME")
	if node == "" {
		node = "unknown"
	}
	pod = os.Getenv("POD_NAME")
	if pod == "" {
		pod = os.Getenv("HOSTNAME")
		if pod == "" {
			pod = "unknown"
		}
	}
	return
}
