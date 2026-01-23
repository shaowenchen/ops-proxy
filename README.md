# Ops Proxy

基于域名的对等（Peer-to-Peer）透明代理系统，支持多节点相互转发。

## 功能

- **对等架构**：所有节点都是对等的 peer，可以相互连接和转发
- **本地转发**：单个节点注册三种域名格式后，可以实现本地转发（反向代理）
- **相互转发**：两个节点可以相互注册对方的域名，实现双向转发
- **多节点通信**：多节点时，可以用公网 IP 连接其他节点，基于公网 IP 也能与其他节点相互通信
- **透明转发**：四层 TCP 透明转发，支持 HTTP/HTTPS 和任意 TCP 协议
- **监控指标**：Prometheus 指标和 Grafana 仪表盘

## 架构

Ops Proxy 采用对等（Peer-to-Peer）架构，所有节点都是对等的 peer，可以相互连接和转发。

**核心概念**：
- 每个 peer 只需要配置 `SERVICE_ADDR`（本地服务）和 `REMOTE_PEER_ADDR`（要连接的 peer）
- 所有 peer 默认监听 `:6443` 端口接收连接和请求
- 通过 domain 自动路由到对应 peer 的服务，实现全网组网

**详细设计文档**：请参考 [design/](./design/) 目录：
- [架构设计](./design/architecture.md) - 系统架构、核心概念和使用场景
- [通信机制](./design/communication.md) - Peer 之间的详细通信流程
- [协议设计](./design/protocol.md) - 通信协议格式和命令说明
- [数据结构](./design/data-structures.md) - 内部数据结构和存储机制

## 配置

配置支持两种方式：YAML 配置文件和环境变量。环境变量会覆盖配置文件中的对应值。

每个 peer 节点可以同时配置：
1. **监听配置**：是否监听端口，接收其他 peer 的请求
2. **连接配置**：连接到哪些其他 peer，注册哪些域名
3. **本地路由**：本地域名到后端的映射（反向代理）

### Peer 配置

```yaml
peer:
  # Listening configuration
  bind_addr: ":6443"                    # Address to listen for peer connections and proxy requests
  listen_address: ":9090"               # Metrics listener address
  telemetry_path: "/metrics"            # Metrics path
  connection_timeout: 5                 # Timeout waiting for peer DATA connection after FORWARD (seconds)
  registration_read_timeout: 180        # Registration connection read timeout (seconds)
  
  # Connection configuration
  remote_peer_addr: ""                  # Comma-separated list of remote peer addresses to connect to (e.g., "peer1:6443,peer2:6443")
  service_addr: ""                      # Comma-separated list of local services to register (e.g., "domain1:backend1:port,domain2:backend2:port")
  peer_name: ""                         # Peer name identifier (optional, defaults to POD_NAME or HOSTNAME)
  reconnect_interval: 5                 # Reconnect interval (seconds)
  max_reconnect: 0                      # Max reconnect attempts (0 means infinite)
  heartbeat_interval: 30                # Heartbeat interval (seconds) - peer re-registers periodically to keep connection alive
```

### Log 配置

```yaml
log:
  level: "info"                       # 日志级别：debug, info, warn, error
  format: "text"                       # 日志格式：text, json
```

### Proxy 配置

```yaml
proxy:
  dial_timeout: 30                     # 拨号超时（秒）
  read_timeout: 30                     # 读取超时（秒）
```

## 环境变量配置

所有配置项都支持通过环境变量设置，环境变量会覆盖配置文件中的值。

### Peer 配置

| 环境变量 | 说明 | 默认值 |
|---------|------|--------|
| `SERVICE_ADDR` | 本地服务地址（逗号分隔，格式：`domain:port` 或 `name:host:port`，例如：`example-cluster.example.com:6443,example-app:example-app.default.svc.cluster.local:80`） | - |
| `REMOTE_PEER_ADDR` | 要连接的其他 peer 地址（逗号分隔，格式：`peer:port`，例如：`peer-b:6443,peer-c:6443`） | - |
| `PEER_NAME` | 本节点名称（可选，默认使用 POD_NAME 或 HOSTNAME） | - |
| `PEER_RECONNECT_INTERVAL_SECONDS` | 重连间隔（秒） | `5` |
| `PEER_MAX_RECONNECT` | 最大重连次数（0 表示无限重试） | `0` |
| `PEER_HEARTBEAT_INTERVAL_SECONDS` | 心跳周期（秒） | `30` |
| `PEER_LISTEN_ADDRESS` | Metrics 监听地址 | `:9090` |
| `PEER_TELEMETRY_PATH` | Metrics 路径 | `/metrics` |
| `PEER_CONNECTION_TIMEOUT_SECONDS` | 连接超时（秒） | `5` |
| `PEER_REGISTRATION_READ_TIMEOUT_SECONDS` | 注册连接读取超时（秒） | `180` |

**说明**：
- 每个 peer 默认监听 `:6443` 端口接收其他 peer 的连接和代理请求（可通过 `PEER_BIND_ADDR` 或 `SERVER_BIND_ADDR` 环境变量覆盖）
- `SERVICE_ADDR`：配置本地可访问的服务，这些服务会注册到所有配置的远程 peer。支持三种格式：`domain:port`（name 和 backend 都是 `domain:port`）、`name:host:port`（name 是 `name`，backend 是 `host:port`）、`name:ip:port`（name 是 `name`，backend 是 `ip:port`）
- `REMOTE_PEER_ADDR`：配置要连接的其他 peer，每个 peer 都会收到本节点的服务注册。支持多个 peer，用逗号分隔。peer 会与这些地址保持持续连接
- `SERVICE_ADDR` 配置后会先注册到当前 peer（本地可转发），若同时配置 `REMOTE_PEER_ADDR` 则会同步到远端 peer
- 旧字段 `SERVER_*` 和 `CLIENT_*` 仍然支持（向后兼容），但建议使用新的 `PEER_*` 字段

### Log 配置

| 环境变量 | 说明 | 默认值 |
|---------|------|--------|
| `LOG_LEVEL` | 日志级别：`debug`, `info`, `warn`, `error` | `info` |
| `LOG_FORMAT` | 日志格式：`text`, `json` | `text` |

### Proxy 配置

| 环境变量 | 说明 | 默认值 |
|---------|------|--------|
| `PROXY_DIAL_TIMEOUT_SECONDS` | 拨号超时（秒） | `30` |
| `PROXY_READ_TIMEOUT_SECONDS` | 读取超时（秒） | `30` |

### `SERVICE_ADDR` 环境变量格式说明

`SERVICE_ADDR` 支持通过环境变量配置多个本地服务，格式说明如下：

- **支持三种格式（逗号分隔）**：
  - **`domain:port`** → name=`domain`, backend=`domain:port`（例如：`mycluster.kubernetes.lb:6443`）
  - **`name:host:port`** → name=`name`, backend=`host:port`（例如：`myapp:myapp.default.svc.cluster.local:80`）
  - **`name:ip:port`** → name=`name`, backend=`ip:port`（例如：`redis:127.0.0.1:6379`）

- **注册协议**：peer 会把它们编码成一行发送给其他 peer：
  - `REGISTER:<name1>:<backend1>,<name2>:<backend2>,...\n`

- **路由方式**：peer 从请求的 **Host/SNI** 提取 `name`，在已注册的 peer 池中找到对应条目，然后通过隧道转发：
  - `FORWARD:proxy_id:name:backend_addr\n`（控制连接）
  - `DATA:proxy_id\n`（数据连接）

- **示例逐条解释**（name → backend）：
  - **`mycluster.kubernetes.lb:6443`** → name=`mycluster.kubernetes.lb`, backend=`mycluster.kubernetes.lb:6443`
  - **`myapp:myapp.default.svc.cluster.local:80`** → name=`myapp`, backend=`myapp.default.svc.cluster.local:80`
  - **`redis:127.0.0.1:6379`** → name=`redis`, backend=`127.0.0.1:6379`

> 说明：`name` 不强制必须是“域名格式”，只要能与客户端请求里的 Host/SNI 完全匹配即可（例如 `myapp` 这种短名也可以）。

## 部署

### 单节点（仅本地转发）

```bash
# 默认运行 peer 模式（监听 + 如果配置了 REMOTE_PEER_ADDR 则连接）
./ops-proxy --config.file=config.yaml
```

配置 `SERVICE_ADDR` 实现本地转发（本地服务会注册到本地 peer）。

### 双节点（相互转发）

**节点 A**：
```bash
./ops-proxy --config.file=config-a.yaml
# 配置示例：
# REMOTE_PEER_ADDR="peer-b.example.com:6443"
# SERVICE_ADDR="example-cluster.example.com:6443,example-app:example-app.default.svc.cluster.local:80"
```

**节点 B**：
```bash
./ops-proxy --config.file=config-b.yaml
# 配置示例：
# REMOTE_PEER_ADDR="peer-a.example.com:6443"
# SERVICE_ADDR="example-cluster-b.example.com:6443,example-app-b:example-app-b.default.svc.cluster.local:80"
```

### 多节点（基于公网 IP）

**节点 A（公网 IP: 192.0.2.1）**：
```bash
./ops-proxy --config.file=config-a.yaml
# REMOTE_PEER_ADDR="192.0.2.2:6443,192.0.2.3:6443"
# SERVICE_ADDR="example-cluster.example.com:6443,example-app-a:example-app-a.default.svc.cluster.local:80"
```

**节点 B（公网 IP: 192.0.2.2）**：
```bash
./ops-proxy --config.file=config-b.yaml
# REMOTE_PEER_ADDR="192.0.2.1:6443"
# SERVICE_ADDR="example-cluster-b.example.com:6443,example-app-b:example-app-b.default.svc.cluster.local:80"
```

**节点 C（内网）**：
```bash
./ops-proxy --config.file=config-c.yaml
# REMOTE_PEER_ADDR="192.0.2.1:6443"
# SERVICE_ADDR="example-app-c:example-app-c.default.svc.cluster.local:80"
```

### Kubernetes

参考 `deploy/` 目录下的部署文件。每个节点都是对等的 peer，通过环境变量配置监听和连接。

## 协议

### 控制连接（与代理端口共用）

- **注册**: `REGISTER:name1:backend1,name2:backend2\n`
- **转发**: `FORWARD:proxy_id:name:backend_addr\n`

### 数据转发

- 每次转发使用独立 DATA 连接：`DATA:proxy_id\n`
- 不解析或修改协议内容

## 构建

```bash
# 使用 vendor 构建
make build

# Docker 构建
docker build -t ops-proxy:latest .
```

## 监控

### Metrics 端点

- **Peer Metrics**: `:9090/metrics` - Peer 指标

### Prometheus 指标

详细的指标说明请参考 `grafana/metrics.md`。

主要指标包括：

**Peer 端（监听）**：
- `ops_proxy_server_info` - Peer 进程信息
- `ops_proxy_clients_total` - 注册的 peer 路由名称总数
- `ops_proxy_clients_connected` - 当前连接的 peer 路由名称数
- `ops_proxy_client_up` - 每个路由名称的连接状态
- `ops_proxy_connections_total` - 代理连接总数
- `ops_proxy_connections_active` - 当前活跃的代理连接数
- `ops_proxy_requests_total` - 代理请求总数（按协议分类）
- `ops_proxy_proxy_errors_total` - 代理错误总数（按原因分类）

**Peer 端（连接）**：
- `ops_proxy_client_info` - Peer 进程信息
- `ops_proxy_client_forwards_total` - Peer 处理的转发请求总数
- `ops_proxy_client_forwards_failed_total` - Peer 失败的转发请求总数
- `ops_proxy_client_active_forwards` - Peer 当前活跃的转发请求数

### Grafana Dashboards

Grafana 仪表盘文件位于 `grafana/` 目录：
- `peer-dashboard.json` - 统一的 Peer 监控面板（包含监听和连接功能的所有指标）

### Prometheus 采集配置

项目提供了 Prometheus Operator 的配置：
- `deploy/servicemonitor.yaml` - Peer ServiceMonitor
- `deploy/podmonitor.yaml` - PodMonitor（采集所有 peer）

## 端口说明

- **:6443** - Peer 代理端口（接收其他 peer 的注册和代理请求，统一端口）
- **:9090** - Metrics 端口（Prometheus / 健康检查 `/healthz`）
