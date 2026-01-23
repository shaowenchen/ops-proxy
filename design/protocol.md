# 协议设计

## 通信协议

使用基于文本的协议，通过 TCP 连接传输，所有命令以 `\n` 结尾。

## 协议命令

| 命令 | 格式 | 说明 |
|------|------|------|
| `REGISTER` | `REGISTER:name1:backend1,name2:backend2\n` | 注册服务列表到远程 peer |
| `SYNC` | `SYNC:name1:backend1,name2:backend2\n` | 同步服务列表给新注册的 peer |
| `FORWARD` | `FORWARD:proxy_id:name:backend\n` | 请求转发命令（控制连接） |
| `DATA` | `DATA:proxy_id\n` | 数据通道标识（数据连接） |
| `OK` | `OK:count\n` | 注册成功确认 |
| `ERROR` | `ERROR:message\n` | 错误响应 |

## 命令详解

### REGISTER 命令

**用途**：Peer A 向 Peer B 注册本地服务列表

**格式**：
```
REGISTER:name1:backend1,name2:backend2,name3:backend3\n
```

**示例**：
```
REGISTER:example-app:example-app.default.svc.cluster.local:80,example-redis:127.0.0.1:6379\n
```

**响应**：
- 成功：`OK:2\n`（数字表示注册的服务数量）
- 失败：`ERROR:Invalid registration\n` 或 `ERROR:No valid registrations\n`

### SYNC 命令

**用途**：Peer B 向 Peer A 同步自己的服务列表（双向同步）

**格式**：
```
SYNC:name1:backend1,name2:backend2,name3:backend3\n
```

**示例**：
```
SYNC:example-app-b:example-app-b.default.svc.cluster.local:80,example-db:192.0.2.100:5432\n
```

**处理**：
- Peer A 收到后更新 `peerServices` map
- 同时注册到 `services` map 用于路由

### FORWARD 命令

**用途**：Peer B 请求 Peer A 转发一个请求到指定的后端

**格式**：
```
FORWARD:proxy_id:name:backend_addr\n
```

**参数**：
- `proxy_id`: 唯一的代理 ID，用于匹配 DATA 连接
- `name`: 服务名称（域名）
- `backend_addr`: 后端地址（host:port）

**示例**：
```
FORWARD:abc123:example-app:example-app.default.svc.cluster.local:80\n
```

**处理流程**：
1. Peer A 收到 FORWARD 命令
2. 创建新的 DATA 连接到 Peer B
3. 发送 `DATA:proxy_id\n` 标识数据连接
4. 连接到本地 backend
5. 双向桥接数据流

### DATA 命令

**用途**：标识数据连接，与 FORWARD 命令中的 proxy_id 匹配

**格式**：
```
DATA:proxy_id\n
```

**示例**：
```
DATA:abc123\n
```

**说明**：
- 每个转发请求使用独立的 DATA 连接
- DATA 连接用于传输实际的请求和响应数据
- 不解析或修改协议内容，完全透明转发

### OK 响应

**用途**：注册成功确认

**格式**：
```
OK:count\n
```

**参数**：
- `count`: 成功注册的服务数量

**示例**：
```
OK:2\n
```

### ERROR 响应

**用途**：错误响应

**格式**：
```
ERROR:message\n
```

**示例**：
```
ERROR:Invalid registration\n
ERROR:No valid registrations\n
```

## 协议流程

### 注册流程

```
Peer A                          Peer B
   |                               |
   |--- REGISTER:svc1:backend1\n -->|
   |                               |--- 解析并存储
   |<-- OK:1\n --------------------|
   |                               |
   |<-- SYNC:svc2:backend2\n ------|
   |--- 更新 peerServices map      |
```

### 转发流程

```
Peer B                          Peer A
   |                               |
   |--- FORWARD:pid:name:backend\n->|
   |                               |--- 创建 DATA 连接
   |<-- DATA:pid\n ----------------|
   |                               |--- 连接 backend
   |<===========================>|
   |    双向数据流（透明转发）      |
```

## 协议特性

- **文本协议**：易于调试和实现
- **行分隔**：所有命令以 `\n` 结尾
- **简单高效**：最小化协议开销
- **可扩展**：易于添加新命令
- **透明转发**：DATA 连接不解析协议内容
