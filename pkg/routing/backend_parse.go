package routing

import (
	"net"
	"strconv"
	"strings"
)

func isPortNumber(s string) bool {
	port, err := strconv.Atoi(s)
	return err == nil && port > 0 && port < 65536
}

// NormalizeBackendAddr normalizes backend address format.
// - If address is only a port number (e.g., "6443"), prepend defaultHost.
// - If address is hostname without port, append ":80".
// Returns normalized address in "host:port" format.
func NormalizeBackendAddr(addr string, defaultHost string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return net.JoinHostPort(defaultHost, "80")
	}

	// Check if address contains a colon (host:port format)
	if strings.Contains(addr, ":") {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return net.JoinHostPort(defaultHost, "80")
		}
		if host == "" {
			host = defaultHost
		}
		return net.JoinHostPort(host, port)
	}

	// No colon, check if it's a valid port number
	if isPortNumber(addr) {
		return net.JoinHostPort(defaultHost, addr)
	}

	// Not a port, treat as hostname and use default port
	return net.JoinHostPort(addr, "80")
}

// ParseBackendAddrString parses the comma-separated service address string format (SERVICE_ADDR).
//
// Supported formats (comma-separated):
//  1) domain:port              -> name=domain, backend=domain:port (e.g., "example-cluster.example.com:6443")
//  2) name:host:port           -> name=name, backend=host:port (e.g., "example-app:example-app.default.svc.cluster.local:80")
//  3) name:ip:port             -> name=name, backend=ip:port (e.g., "example-redis:127.0.0.1:6379")
//
// If an item has no ":" (legacy fallback), defaultName is used as the routing name.
func ParseBackendAddrString(s string, defaultName string) []BackendConfig {
	out := make([]BackendConfig, 0)
	if strings.TrimSpace(s) == "" {
		return out
	}

	parts := strings.Split(s, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		idx := strings.Index(part, ":")
		if idx == -1 {
			out = append(out, BackendConfig{
				Name:    strings.TrimSpace(defaultName),
				Address: part,
			})
			continue
		}

		name := strings.TrimSpace(part[:idx])
		backend := strings.TrimSpace(part[idx+1:])
		if name == "" {
			name = strings.TrimSpace(defaultName)
		}

		// Case 1: name:port => name:port
		if backend != "" && !strings.Contains(backend, ":") && isPortNumber(backend) {
			backend = net.JoinHostPort(name, backend)
		} else if backend != "" && !strings.Contains(backend, ":") {
			// Case 2: name:host => host:80
			backend = net.JoinHostPort(backend, "80")
		} else if backend == "" {
			backend = net.JoinHostPort("localhost", "80")
		}

		out = append(out, BackendConfig{
			Name:    name,
			Address: backend,
		})
	}

	return out
}

