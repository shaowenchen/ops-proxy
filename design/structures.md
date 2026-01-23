# 数据结构设计

## 服务列表存储结构

系统维护两个独立的 map 结构，用于不同的用途：

### 1. `services` map（路由表）

**用途**：用于路由查找，快速定位服务对应的 peer 连接

**Key 格式**：`name@source_ip`

**示例**：
- `example-app@192.0.2.10`
- `example-redis@192.0.2.1`

**Value 类型**：`*types.ClientInfo`

**ClientInfo 结构**：
```go
type ClientInfo struct {
    Name        string      // 服务名称（域名）
    IP          string      // 来源 peer 的 IP 地址
    BackendAddr string      // 后端地址（host:port）
    Conn        net.Conn    // 控制连接（用于发送 FORWARD 命令）
    LastSeen    time.Time   // 最后更新时间
    Connected   bool        // 连接状态
    ConnMu      *sync.Mutex // 连接互斥锁（多个服务可共享一个连接）
}
```

**特点**：
- 用于快速路由查找
- Key 包含服务名称和来源 IP，支持同一服务来自不同 peer
- 存储了控制连接，用于发送 FORWARD 命令

### 2. `peerServices` map（Peer 服务列表）

**用途**：查看每个 peer 的完整服务列表，便于调试和监控

**Key 格式**：`peer_ip`

**示例**：
- `192.0.2.10`
- `192.0.2.1`

**Value 类型**：`*types.PeerServices`

**PeerServices 结构**：
```go
type PeerServices struct {
    PeerIP   string                    // Peer 的 IP 地址
    Services map[string]*ServiceInfo   // 该 peer 的所有服务
    LastSync int64                     // 最后同步时间戳
}
```

**ServiceInfo 结构**：
```go
type ServiceInfo struct {
    Name        string // 服务名称（域名）
    BackendAddr string // 后端地址
    SourcePeer  string // 来源 peer IP
}
```

**特点**：
- 按 peer 组织服务列表
- 便于查看每个 peer 的完整服务清单
- 记录最后同步时间，便于监控

## Map 更新机制

### 更新时机

1. **本地服务注册**：
   - 启动时注册 `SERVICE_ADDR` 中的服务
   - 服务注册到 `services` map，source 为 `"local"`

2. **远程服务注册**：
   - 收到 `REGISTER` 命令时，注册到 `services` map
   - source 为远程 peer 的 IP

3. **服务同步**：
   - 收到 `SYNC` 命令时，更新 `peerServices` map
   - 同时注册到 `services` map 用于路由

### 更新后的操作

每次更新 map 时，系统会自动：
1. 打印完整的 `services` map 结构
2. 打印完整的 `peerServices` map 结构
3. 更新相关指标

## 打印格式

### services map 打印格式

```
[registry] services peer_id=xxx svc_map={total=3}
[registry] services peer_id=xxx   | peer | name | backend |
[registry] services peer_id=xxx   | ---- | ---- | ------- |
[registry] services peer_id=xxx   | local | service1 | 127.0.0.1:80 |
[registry] services peer_id=xxx   | 192.0.2.10 | example-service2 | 192.0.2.10:8080 |
```

### peerServices map 打印格式

```
[registry] peer_services_map peer_id=xxx {total_peers=2}
[registry] peer_services_map peer_id=xxx   peer=192.0.2.10 {services=2 last_sync=1234567890}
[registry] peer_services_map peer_id=xxx     | name | backend |
[registry] peer_services_map peer_id=xxx     | ---- | ------- |
[registry] peer_services_map peer_id=xxx     | service1 | 127.0.0.1:80 |
[registry] peer_services_map peer_id=xxx     | example-service2 | 192.0.2.100:8080 |
```

## 并发安全

- `services` map 使用 `clientsLock sync.RWMutex` 保护
- `peerServices` map 使用 `peerServicesLock sync.RWMutex` 保护
- 所有读写操作都需要获取相应的锁

## 数据一致性

- 服务注册时同时更新两个 map
- 服务同步时同时更新 `peerServices` 和 `services` map
- 每次更新后立即打印，便于验证一致性
