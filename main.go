package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ops-proxy/pkg/client"
	"github.com/ops-proxy/pkg/config"
	"github.com/ops-proxy/pkg/logging"
	"github.com/ops-proxy/pkg/server"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	configFile    = kingpin.Flag("config.file", "Path to configuration file.").Default("config.yaml").String()
	listenAddress = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9090").String()
	telemetryPath = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
	bindAddr      = kingpin.Flag("bind-addr", "Address to bind for proxy (listening)").Default(":6443").String()

	// Global config
	appConfig *config.Config
)

func main() {
	kingpin.Parse()

	// Load configuration
	var err error
	appConfig, err = config.LoadConfig(*configFile)
	if err != nil {
		// If config file doesn't exist, continue with defaults
		logging.Logf("Warning: Failed to load config file: %v, using defaults", err)
		appConfig = &config.Config{}
		appConfig.SetDefaults()
		appConfig.ApplyEnvOverrides()
	}

	// Initialize peer ID early
	peerID := logging.GetPeerID()
	logging.Logf("Peer initialized with ID: %s", peerID)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		logging.Log("Received shutdown signal, shutting down gracefully...")
		cancel()
	}()

	// Run as peer: always listens, and connects to other peers if REMOTE_PEER_ADDR is configured
	if err := runPeer(ctx); err != nil {
		logging.Fatalf("Peer error: %v", err)
	}
}

func runPeer(ctx context.Context) error {
	// Create peer (using server for listening)
	proxyServer, err := server.NewProxyServer(appConfig)
	if err != nil {
		return fmt.Errorf("failed to create peer: %v", err)
	}

	// Get bind address from command line or config
	bindAddress := *bindAddr
	if appConfig != nil && appConfig.Peer.BindAddr != "" {
		bindAddress = appConfig.Peer.BindAddr
	}

	// Start proxy listener (always listen)
	go func() {
		if err := proxyServer.StartProxyListener(bindAddress, appConfig); err != nil {
			logging.Fatalf("Proxy listener error: %v", err)
		}
	}()

	// Register local services if SERVICE_ADDR is configured
	// Design requirement: if SERVICE_ADDR is empty, don't register any service (no default value)
	go func() {
		peerName := appConfig.Peer.PeerName
		if peerName == "" {
			peerName = os.Getenv("POD_NAME")
			if peerName == "" {
				peerName = os.Getenv("HOSTNAME")
			}
		}
		if peerName == "" {
			peerName = "local"
		}
		
		serviceAddr := appConfig.Peer.ServiceAddr
		if serviceAddr == "" && appConfig.Peer.BackendAddr != "" {
			serviceAddr = appConfig.Peer.BackendAddr
		}
		
		// Only register services if SERVICE_ADDR is configured
		if serviceAddr == "" {
			logging.Logf("No SERVICE_ADDR configured, skipping local service registration")
		} else {
			registered := proxyServer.RegisterLocalServices(serviceAddr, peerName)
			if registered > 0 {
				logging.Logf("Registered %d local service(s) from SERVICE_ADDR", registered)
			} else {
				logging.Logf("No valid services in SERVICE_ADDR: %q", serviceAddr)
			}
		}
		
		// Print full service map after local services are registered
		proxyServer.LogServicesTable()
		
		// Connect to remote peers if configured
		if appConfig != nil && appConfig.Peer.RemotePeerAddr != "" {
			if err := client.Run(appConfig, "", peerName, serviceAddr, proxyServer); err != nil {
				logging.Logf("Peer connections error: %v", err)
			}
		}
	}()

	// Get metrics config from command line or config file
	metricsPath := *telemetryPath
	metricsAddr := *listenAddress
	if appConfig != nil {
		if appConfig.Peer.TelemetryPath != "" {
			metricsPath = appConfig.Peer.TelemetryPath
		}
		if appConfig.Peer.ListenAddress != "" {
			metricsAddr = appConfig.Peer.ListenAddress
		}
	}

	// Start metrics server
	return proxyServer.StartMetricsServer(metricsAddr, metricsPath)
}
