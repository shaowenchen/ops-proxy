# Peer 通信机制详解

两个 peer 之间的通信分为以下几个阶段：

## 1. 连接建立阶段

Peer A 通过 IP 192.0.2.1:6443 对外暴露。

*Peer A*

- 注册有 test-cluster-a.example.com:80 服务

*Peer B*

- 注册有 test-cluster-b.example.com:80 服务
- 通过 remote_peer_addr=192.0.2.1:6443 连接到 Peer A

*Peer C*

- 注册有 test-cluster-c.example.com:80 服务
- 通过 remote_peer_addr=192.0.2.1:6443 连接到 Peer A

*Peer B 和 Peer C 连接到 Peer A*

- 保持持续连接，不需要频繁建立连接和断开连接
- 转发流量时，生成 8 位随机 proxy_id 进行唯一标识，避免数据混乱
- 保持心跳连接，每隔 3 秒发送一个心跳包，如果 30 秒内没有收到心跳包，则认为 Peer 离线，重新同步 services 列表

## 2. 服务注册阶段

### 2.1 Peer 的相互发现

通过 REMOTE_PEER_ADDR 配置，Peer 可以相互发现，REMOTE_PEER_ADDR 可以配置多个

- 只有通过 REMOTE_PEER_ADDR 配置连接的 Peer 相互自己本地注册的 svc 列表，转发流量
- Peer B 和 Peer C 看不到彼此的本地 svc 列表，但是 Peer A 可以看到 Peer B 和 Peer C 的本地 svc 列表

### 2.2 Peer services 同步

- Peer B 和 Peer C 会定期发送 REGISTER 命令到 Peer A，用于更新整个 peer 集群的服务列表
- Peer A 会推送整个 peer 集群的服务列表到 Peer B 和 Peer C

**Peer → Peer 同步 Services 列表流程**：

1. Peer A 收到 SYNC 命令
2. 更新 peerServices map（存储 Peer B 的完整服务列表）
3. 同时注册到 services map（用于路由查找）
4. 打印完整的 peerServices map 结构

链接其他 Peer 时，可选使用 REMOTE_PEER_ADDR 配置代理地址，用于跨区域通信。

每个 Peer 都可能有一个 REMOTE_PEER_ADDR 配置，用于提供给其他 Peer 连接到自己。

REMOTE_PEER_ADDR 只是用于连接到其他 Peer，不作为 Peer 的唯一标识。


## 3. 数据转发阶段

### 3.1 转发模式

Peer 只进行两种转发:

- 如果是本地的 svc，直接转发到本地 backend
- 如果是其他 Peer 的 svc，转发到其他 Peer

### 3.2 转发示例

在转发过程中，连接是持续保持的

- Peer A 收到 test-cluster-a.example.com:80 的请求

```
1. Peer A 收到 test-cluster-a.example.com:80 的请求
2. 根据 Host/SNI 提取域名，在 services map 中查找服务，发现是本地的 local Peer A 的服务，直接转发到本地 backend
```

- Peer B 收到 test-cluster-a.example.com:80 的请求

```
1. Peer B 收到 test-cluster-b.example.com:80 的请求
2. 根据 Host/SNI 提取域名，在 services map 中查找服务，发现是其他 Peer A 的服务
3. 生成唯一的 proxy_id，向 Peer A 发送 FORWARD 命令：
   FORWARD:proxy_id:test-cluster-a.example.com:80\n
4. Peer A 收到 FORWARD 命令后，转发请求到 backend test-cluster-a.example.com:80
5. Peer A 收到 backend test-cluster-a.example.com:80 的响应后，转发响应到 Peer B
6. Peer B 收到 backend test-cluster-a.example.com:80 的响应后，转发响应到客户端
```

- Peer A 收到 test-cluster-c.example.com:80 的请求
```
1. Peer A 收到 test-cluster-c.example.com:80 的请求
2. 根据 Host/SNI 提取域名，在 services map 中查找服务，发现是其他 Peer C 的服务
3. 生成唯一的 proxy_id，向 Peer C 发送 FORWARD 命令：
   FORWARD:proxy_id:test-cluster-c.example.com:80\n
4. Peer C 收到 FORWARD 命令后，转发请求到 backend test-cluster-c.example.com:80
5. Peer C 收到 backend test-cluster-c.example.com:80 的响应后，转发响应到 Peer A
6. Peer A 收到 backend test-cluster-c.example.com:80 的响应后，转发响应到客户端
```
