# Ops Proxy Metrics

This file lists the Prometheus metrics exported by Ops Proxy.

Notes:
- `client_name` label is the **registered routing name** (matched against Host/SNI).
- `proxy_id` is **not** a Prometheus label (too high-cardinality). It's only used in the tunnel protocol/logs.
- Exported label dimensions on the textfile/HTTP exposition are **`namespace`**, **`node`**, **`pod`** (plus `client_name` / `protocol` / `reason` where applicable). The **`cluster`** label is **not** set by the application; it is expected to be attached at **scrape time** (e.g. Google Managed Prometheus, Prometheus Operator target labels). Set **`POD_NAMESPACE`** via Kubernetes downward API if you want `namespace` on-series when not relying on target relabeling alone; otherwise it defaults to `unknown`.
- All peers can act as both listening (receiving registrations) and connecting (registering services) nodes.

## Peer registry

- **`ops_proxy_server_info{namespace,node,pod}`** (gauge)  
  Peer listening process info metric (always 1). Useful for Grafana variables / target discovery.
  Present when peer is listening for connections.

- **`ops_proxy_clients_total{namespace,node,pod}`** (gauge)  
  Total number of registered routing names (services) in this listening peer instance.

- **`ops_proxy_clients_connected{namespace,node,pod}`** (gauge)  
  Number of currently connected routing names (services) in this listening peer instance.

- **`ops_proxy_client_info{namespace,node,pod}`** (gauge)  
  Client-side process marker (always 1). Exported on the unified `/metrics` endpoint so dashboards can discover pods that run the outbound peer client.

- **`ops_proxy_client_up{client_name,namespace,node,pod}`** (gauge)  
  Per-name connection status: 1 = connected, 0 = disconnected.

- **`ops_proxy_client_registrations_total{client_name,namespace,node,pod}`** (counter)  
  Total REGISTER refreshes received for a routing name (service).

- **`ops_proxy_client_disconnects_total{client_name,namespace,node,pod}`** (counter)  
  Total disconnect/unregister events for a routing name (service).

### Client-side forwarding (outbound peer handles `FORWARD` from remote)

- **`ops_proxy_client_forwards_total{client_name,namespace,node,pod}`** (counter)  
  Completed forward handlers on the connecting peer (per routing name).

- **`ops_proxy_client_forwards_failed_total{client_name,reason,namespace,node,pod}`** (counter)  
  Failed forward handlers (`backend_dial_error`, `data_dial_error`, `data_header_error`, `io_error`, …).

- **`ops_proxy_client_active_forwards{client_name,namespace,node,pod}`** (gauge)  
  In-flight forward handlers.

## Proxy traffic (listening peer side)

Label set on export: `client_name`, `namespace`, `node`, `pod` (plus `protocol` where noted). **`cluster`** is added by the scrape configuration.

- **`ops_proxy_connections_total`** (counter)  
  Total number of proxy connections routed to a name.

- **`ops_proxy_connections_active`** (gauge)  
  Current number of in-flight proxy requests for a name.

- **`ops_proxy_connections_failed_total`** (counter)  
  Total number of failed proxy connections for a name.

- **`ops_proxy_connections_bytes_tx_total`** (counter)  
  Total bytes transmitted (server → client → backend direction, aggregated).

- **`ops_proxy_connections_bytes_rx_total`** (counter)  
  Total bytes received (backend → client → server direction, aggregated).

- **`ops_proxy_requests_total{client_name,protocol,...}`** (counter)  
  Total number of proxy requests by protocol (`http` / `https` / `tcp` / `forward` / `http_connect`).

- **`ops_proxy_requests_success_total`** (counter)  
  Successful proxy requests.

- **`ops_proxy_requests_failed_total`** (counter)  
  Failed proxy requests.

- **`ops_proxy_latency_seconds`** (gauge)  
  Average latency in seconds (derived from internal sum/count).

## Tunnel / proxy-id (listening peer side)

- **`ops_proxy_forward_commands_total`** (counter)  
  Total FORWARD commands sent (each proxy request should generate one).

- **`ops_proxy_data_connections_matched_total`** (counter)  
  DATA connections successfully matched to a pending `proxy_id`.

- **`ops_proxy_data_connections_timeout_total`** (counter)  
  Timeouts waiting for a DATA connection after sending FORWARD.

- **`ops_proxy_data_connections_unexpected_total{namespace,node,pod}`** (counter)  
  DATA connections that arrived with no pending `proxy_id` on the server.

- **`ops_proxy_pending_data_connections{namespace,node,pod}`** (gauge)  
  Current number of pending proxy-ids waiting for DATA connection.

## Proxy / tunnel errors (listening peer side)

- **`ops_proxy_proxy_errors_total{client_name,reason,namespace,node,pod}`** (counter)  
  Error counter by reason. Examples:
  - `no_client`, `remote_not_connected`, `remote_conn_nil`, `remote_conn_closed`
  - `backend_dial_error`, `backend_io_error`, `empty_backend`
  - `forward_conn_nil`, `forward_conn_closed`, `forward_write_error`
  - `data_timeout`, `data_io_error`, `data_dial_error`, `data_header_error`
  - `forward_io_error` (registration-side handler)

