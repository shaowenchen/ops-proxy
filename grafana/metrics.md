# Ops Proxy Metrics

This file lists the Prometheus metrics exported by Ops Proxy.

Notes:
- `client_name` label is the **registered routing name** (matched against Host/SNI).
- `proxy_id` is **not** a Prometheus label (too high-cardinality). It's only used in the tunnel protocol/logs.
- Most metrics include `node` and `pod` labels for scoping in Kubernetes.
- All peers can act as both listening (receiving registrations) and connecting (registering services) nodes.

## Peer registry

- **`ops_proxy_server_info{node,pod}`** (gauge)  
  Peer listening process info metric (always 1). Useful for Grafana variables / target discovery.
  Present when peer is listening for connections.

- **`ops_proxy_clients_total{node,pod}`** (gauge)  
  Total number of registered routing names (services) in this listening peer instance.

- **`ops_proxy_clients_connected{node,pod}`** (gauge)  
  Number of currently connected routing names (services) in this listening peer instance.

- **`ops_proxy_client_info{node,pod}`** (gauge)  
  Peer connecting process info metric (always 1). Useful for Grafana variables / target discovery.
  Present when peer is connecting to other peers.

- **`ops_proxy_client_up{client_name,node,pod}`** (gauge)  
  Per-name connection status: 1 = connected, 0 = disconnected.

- **`ops_proxy_client_registrations_total{client_name,node,pod}`** (counter)  
  Total REGISTER refreshes received for a routing name (service).

- **`ops_proxy_client_disconnects_total{client_name,node,pod}`** (counter)  
  Total disconnect/unregister events for a routing name (service).

## Proxy traffic (listening peer side)

- **`ops_proxy_connections_total{client_name,node,pod}`** (counter)  
  Total number of proxy connections routed to a name.

- **`ops_proxy_connections_active{client_name,node,pod}`** (gauge)  
  Current number of in-flight proxy requests for a name.

- **`ops_proxy_connections_failed_total{client_name,node,pod}`** (counter)  
  Total number of failed proxy connections for a name.

- **`ops_proxy_connections_bytes_tx_total{client_name,node,pod}`** (counter)  
  Total bytes transmitted (server → client → backend direction, aggregated).

- **`ops_proxy_connections_bytes_rx_total{client_name,node,pod}`** (counter)  
  Total bytes received (backend → client → server direction, aggregated).

- **`ops_proxy_requests_total{client_name,protocol,node,pod}`** (counter)  
  Total number of proxy requests by protocol (`http` / `https` / `tcp`).

- **`ops_proxy_requests_success_total{client_name,protocol,node,pod}`** (counter)  
  Successful proxy requests.

- **`ops_proxy_requests_failed_total{client_name,protocol,node,pod}`** (counter)  
  Failed proxy requests.

- **`ops_proxy_latency_seconds{client_name,protocol,node,pod}`** (gauge)  
  Average latency in seconds (derived from internal sum/count).

## Tunnel / proxy-id (listening peer side)

- **`ops_proxy_forward_commands_total{client_name,node,pod}`** (counter)  
  Total FORWARD commands sent (each proxy request should generate one).

- **`ops_proxy_data_connections_matched_total{client_name,node,pod}`** (counter)  
  DATA connections successfully matched to a pending `proxy_id`.

- **`ops_proxy_data_connections_timeout_total{client_name,node,pod}`** (counter)  
  Timeouts waiting for a DATA connection after sending FORWARD.

- **`ops_proxy_data_connections_unexpected_total{node,pod}`** (counter)  
  DATA connections that arrived with no pending `proxy_id` on the server.

- **`ops_proxy_pending_data_connections{node,pod}`** (gauge)  
  Current number of pending proxy-ids waiting for DATA connection.

## Proxy / tunnel errors (listening peer side)

- **`ops_proxy_proxy_errors_total{client_name,reason,node,pod}`** (counter)  
  Error counter by reason. Current reasons:
  - `no_client`
  - `forward_write_error`
  - `data_timeout`
  - `data_io_error`

