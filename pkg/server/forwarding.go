package server

import (
	"io"
	"net"
	"time"

	"github.com/ops-proxy/pkg/config"
	"github.com/ops-proxy/pkg/types"
)

// ForwardingHandler handles forwarding logic for both local and remote services
type ForwardingHandler struct {
	server *ProxyServer
}

// NewForwardingHandler creates a new forwarding handler
func NewForwardingHandler(server *ProxyServer) *ForwardingHandler {
	return &ForwardingHandler{
		server: server,
	}
}

// ForwardLocal forwards traffic to a local backend service
// This is used when the service is registered locally (IP="local")
// Design doc: forward directly to local backend
func (h *ForwardingHandler) ForwardLocal(
	srcReader io.Reader,
	srcConn net.Conn,
	name, protocol, backendAddr string,
	cfg *config.Config,
	updateMetrics func(string, string, bool, int64, int64, time.Duration),
) {
	h.server.forwardDirect(srcReader, srcConn, name, protocol, backendAddr, cfg, updateMetrics)
}

// ForwardRemote forwards traffic to a remote peer via tunnel
// This is used when the service is registered on a remote peer
// Design doc: generate unique proxy_id, send FORWARD command to peer
func (h *ForwardingHandler) ForwardRemote(
	srcReader io.Reader,
	srcConn net.Conn,
	name, protocol string,
	client *types.ClientInfo,
	cfg *config.Config,
	updateMetrics func(string, string, bool, int64, int64, time.Duration),
	backendOverride string,
) {
	h.server.forwardOnce(srcReader, srcConn, name, protocol, client, cfg, updateMetrics, backendOverride)
}
