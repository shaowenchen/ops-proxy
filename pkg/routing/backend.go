package routing

// BackendConfig is a reusable {name,address} mapping used by both client and server config.
type BackendConfig struct {
	Name    string `yaml:"name"`    // Routed name (Host/SNI)
	Address string `yaml:"address"` // Backend address (host:port)
}

