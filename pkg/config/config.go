package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ops-proxy/pkg/routing"
	"gopkg.in/yaml.v3"
)

// Config application configuration structure
type Config struct {
	Peer  PeerConfig  `yaml:"peer"`
	Log   LogConfig   `yaml:"log"`
	Proxy ProxyConfig `yaml:"proxy"`
}

// PeerConfig peer configuration (listening and connecting)
type PeerConfig struct {
	// Listening configuration
	BindAddr                string `yaml:"bind_addr"`                 // Local peer listening address (format: ip:port or :port, e.g., "127.0.0.1:6443" or ":6443")
	LocalPeerAddr           string `yaml:"local_peer_addr"`           // Local peer address for other peers to connect (optional, e.g., "lb.example.com:6443"). If not set, uses POD_IP:port
	ListenAddress           string `yaml:"listen_address"`            // Metrics listener address
	TelemetryPath           string `yaml:"telemetry_path"`            // Metrics path
	ConnectionTimeout       int    `yaml:"connection_timeout"`        // Timeout waiting for peer DATA connection after FORWARD (seconds)
	RegistrationReadTimeout int    `yaml:"registration_read_timeout"` // Registration connection read timeout in seconds

	// Connection configuration
	RemotePeerAddr string `yaml:"remote_peer_addr"` // Comma-separated list of remote peer addresses to connect to (e.g., "peer1:6443,peer2:6443")
	ServiceAddr    string `yaml:"service_addr"`     // Comma-separated list of local services to register (e.g., "example-cluster.example.com:6443,example-app:example-app.default.svc.cluster.local:80")
	PeerName       string `yaml:"peer_name"`        // Peer name identifier (optional, defaults to POD_NAME or HOSTNAME)
	// Legacy field (for backward compatibility)
	PeerAddr          string `yaml:"peer_addr"`          // Legacy: use remote_peer_addr instead
	ReconnectInterval int    `yaml:"reconnect_interval"` // Reconnect interval in seconds
	MaxReconnect      int    `yaml:"max_reconnect"`      // Max reconnect attempts (0 means infinite)
	HeartbeatInterval int    `yaml:"heartbeat_interval"` // Heartbeat interval in seconds

	// Legacy fields (for backward compatibility)
	ServerAddr  string                  `yaml:"server_addr"`
	BackendAddr string                  `yaml:"backend_addr"`
	Backends    []routing.BackendConfig `yaml:"backends"`
	ClientName  string                  `yaml:"client_name"`
}

// LogConfig log configuration
type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// ProxyConfig proxy configuration
type ProxyConfig struct {
	DialTimeout int `yaml:"dial_timeout"`
	ReadTimeout int `yaml:"read_timeout"`
}

// LoadConfig loads configuration from file
func LoadConfig(configPath string) (*Config, error) {
	if configPath == "" {
		// Try default path
		configPath = "config.yaml"
	}

	// Check if file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file not found: %s", configPath)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %v", err)
	}

	// Set default values
	config.SetDefaults()

	// Apply environment variable overrides
	config.ApplyEnvOverrides()

	return &config, nil
}

// SetDefaults sets default values
func (c *Config) SetDefaults() {
	if c.Peer.BindAddr == "" {
		c.Peer.BindAddr = ":6443"
	}
	if c.Peer.ListenAddress == "" {
		c.Peer.ListenAddress = ":9090"
	}
	if c.Peer.TelemetryPath == "" {
		c.Peer.TelemetryPath = "/metrics"
	}
	if c.Peer.ConnectionTimeout == 0 {
		c.Peer.ConnectionTimeout = 30 // Increased from 5s to 30s for better reliability
	}
	if c.Peer.RegistrationReadTimeout == 0 {
		c.Peer.RegistrationReadTimeout = 30 // Design doc: 30s timeout to detect peer offline
	}

	if c.Peer.ReconnectInterval == 0 {
		c.Peer.ReconnectInterval = 5
	}
	if c.Peer.HeartbeatInterval == 0 {
		c.Peer.HeartbeatInterval = 3 // Design doc: send heartbeat every 3 seconds
	}

	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "text"
	}

	if c.Proxy.DialTimeout == 0 {
		c.Proxy.DialTimeout = 30
	}
	if c.Proxy.ReadTimeout == 0 {
		c.Proxy.ReadTimeout = 30
	}
}

// GetConnectionTimeout gets connection timeout
func (c *Config) GetConnectionTimeout() time.Duration {
	return time.Duration(c.Peer.ConnectionTimeout) * time.Second
}

// GetRegistrationReadTimeout gets registration connection read timeout
func (c *Config) GetRegistrationReadTimeout() time.Duration {
	return time.Duration(c.Peer.RegistrationReadTimeout) * time.Second
}

// GetDialTimeout gets dial timeout
func (c *Config) GetDialTimeout() time.Duration {
	return time.Duration(c.Proxy.DialTimeout) * time.Second
}

// GetReadTimeout gets read timeout
func (c *Config) GetReadTimeout() time.Duration {
	return time.Duration(c.Proxy.ReadTimeout) * time.Second
}

// GetReconnectInterval gets reconnect interval
func (c *Config) GetReconnectInterval() time.Duration {
	return time.Duration(c.Peer.ReconnectInterval) * time.Second
}

// GetHeartbeatInterval gets peer heartbeat interval
func (c *Config) GetHeartbeatInterval() time.Duration {
	return time.Duration(c.Peer.HeartbeatInterval) * time.Second
}

// ApplyEnvOverrides applies environment variable overrides
func (c *Config) ApplyEnvOverrides() {
	// Peer listening config (bind address, defaults to :6443)
	// Can be overridden via PEER_BIND_ADDR or SERVER_BIND_ADDR for backward compatibility
	if val := os.Getenv("PEER_BIND_ADDR"); val != "" {
		c.Peer.BindAddr = val
	} else if val := os.Getenv("SERVER_BIND_ADDR"); val != "" {
		c.Peer.BindAddr = val
	}
	if val := os.Getenv("PEER_LISTEN_ADDRESS"); val != "" {
		c.Peer.ListenAddress = val
	} else if val := os.Getenv("SERVER_LISTEN_ADDRESS"); val != "" {
		c.Peer.ListenAddress = val
	}
	if val := os.Getenv("PEER_TELEMETRY_PATH"); val != "" {
		c.Peer.TelemetryPath = val
	} else if val := os.Getenv("SERVER_TELEMETRY_PATH"); val != "" {
		c.Peer.TelemetryPath = val
	}
	if val := os.Getenv("PEER_CONNECTION_TIMEOUT_SECONDS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			c.Peer.ConnectionTimeout = i
		}
	} else if val := os.Getenv("SERVER_CONNECTION_TIMEOUT_SECONDS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			c.Peer.ConnectionTimeout = i
		}
	}
	if val := os.Getenv("PEER_REGISTRATION_READ_TIMEOUT_SECONDS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			c.Peer.RegistrationReadTimeout = i
		}
	} else if val := os.Getenv("SERVER_REGISTRATION_READ_TIMEOUT_SECONDS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			c.Peer.RegistrationReadTimeout = i
		}
	}

	// Peer connection config
	if val := os.Getenv("REMOTE_PEER_ADDR"); val != "" {
		c.Peer.RemotePeerAddr = val
	}
	if val := os.Getenv("LOCAL_PEER_ADDR"); val != "" {
		c.Peer.LocalPeerAddr = val
	}
	if val := os.Getenv("SERVICE_ADDR"); val != "" {
		c.Peer.ServiceAddr = val
	} else if val := os.Getenv("SRV_ADDR"); val != "" {
		// Legacy: support SRV_ADDR for backward compatibility
		c.Peer.ServiceAddr = val
	}
	if val := os.Getenv("PEER_NAME"); val != "" {
		c.Peer.PeerName = val
	} else if val := os.Getenv("CLIENT_NAME"); val != "" {
		c.Peer.PeerName = val
	}
	// Legacy fields (for backward compatibility)
	if val := os.Getenv("CLIENT_SERVER_ADDR"); val != "" {
		c.Peer.ServerAddr = val
		// If REMOTE_PEER_ADDR not set, use CLIENT_SERVER_ADDR as remote peer
		if c.Peer.RemotePeerAddr == "" {
			c.Peer.RemotePeerAddr = val
		}
	}
	if val := os.Getenv("CLIENT_BACKEND_ADDR"); val != "" {
		c.Peer.BackendAddr = val
		// If SRV_ADDR not set, use CLIENT_BACKEND_ADDR as service
		if c.Peer.ServiceAddr == "" {
			c.Peer.ServiceAddr = val
		}
	}
	if val := os.Getenv("PEER_RECONNECT_INTERVAL_SECONDS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			c.Peer.ReconnectInterval = i
		}
	} else if val := os.Getenv("CLIENT_RECONNECT_INTERVAL_SECONDS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			c.Peer.ReconnectInterval = i
		}
	}
	if val := os.Getenv("PEER_MAX_RECONNECT"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			c.Peer.MaxReconnect = i
		}
	} else if val := os.Getenv("CLIENT_MAX_RECONNECT"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			c.Peer.MaxReconnect = i
		}
	}
	if val := os.Getenv("PEER_HEARTBEAT_INTERVAL_SECONDS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			c.Peer.HeartbeatInterval = i
		}
	} else if val := os.Getenv("CLIENT_HEARTBEAT_INTERVAL_SECONDS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			c.Peer.HeartbeatInterval = i
		}
	}

	// Log config
	if val := os.Getenv("LOG_LEVEL"); val != "" {
		c.Log.Level = strings.ToLower(val)
	}
	if val := os.Getenv("LOG_FORMAT"); val != "" {
		c.Log.Format = strings.ToLower(val)
	}

	// Proxy config
	if val := os.Getenv("PROXY_DIAL_TIMEOUT_SECONDS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			c.Proxy.DialTimeout = i
		}
	}
	if val := os.Getenv("PROXY_READ_TIMEOUT_SECONDS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			c.Proxy.ReadTimeout = i
		}
	}
}
