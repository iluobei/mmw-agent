# mmw-agent

妙妙屋X 远程服务器代理程序。部署在子服务器上，负责与主控（miaomiaowux）通信，上报流量/速度数据，并接受主控的远程管理指令。

<details>
<summary>更新日志</summary>

### v0.2.1 (2026-05-30)
- 🌈优化主控使用CDN情况下的Agent互联

### v0.2.0 (2026-05-29)
- 🌈 优化绑定套餐逻辑

### v0.1.9 (2026-05-29)
- 🛠️ fix:tg通知风暴

### v0.1.8 (2026-05-29)
- 🛠️ fix:优先上报ipv4

### v0.1.7 (2026-05-29)
- 🛠️ fix:OpenRC LXC 双开 mmw-agent

### v0.1.6 (2026-05-29)
- 🛠️ fix:偶发重复复制入站

### v0.1.5 (2026-05-29)
- 🛠️ fix:更新版本失败

### v0.1.4 (2026-05-29)
- 🛠️ fix:更新版本失败

### v0.1.3 (2026-05-29)
- 🛠️ fix:发布时没有更新版本

### v0.0.21 (2026-05-29)
- 🛠️ fix:支持版本上报

### v0.0.20 (2026-05-29)
- 🛠️ fix:不用虚拟tag
- 🛠️ fix:批量绑定套餐的并发问题

### v0.0.19 (2026-05-29)
- 🛠️ fix:某些环境Agent无法升级
- 🛠️ fix:某些用户自己添加的节点没有tag导致修改失败

### v0.0.18 (2026-05-28)
- 🛠️ fix:编辑出站后乱序

### v0.0.17 (2026-05-27)
- 🛠️ fix:升级有概率重启失败

### v0.0.16 (2026-05-27)
- 🛠️ fix:升级有概率重启失败

### v0.0.15 (2026-05-27)
- 🛠️ fix:升级有概率重启失败

### v0.0.14 (2026-05-27)
- 🛠️ fix:升级有概率重启失败

### v0.0.13 (2026-05-27)
- 🛠️ fix:内联模式偶发切换了不起作用

### v0.0.12 (2026-05-27)
- 🛠️ fix:重复安装覆盖配置

### v0.0.11 (2026-05-26)
- 🌈 初始化内联xray时合并配置
- 🌈 支持切换端口号

### v0.0.10 (2026-05-22)
- 🌈 支持路由负载均衡配置

### v0.0.9 (2026-05-21)
- 🛠️ fix:vision limiter fork

### v0.0.8 (2026-05-21)
- 🌈 增加缺少的错误信息提示

### v0.0.7 (2026-05-21)
- 🌈 增加打包配置
- 🌈 支持调整流量上报间隔
- 🛠️ fix:vision限速失败
- 🛠️ fix:流量数据上报缺少节点级别数据
- 🛠️ fix:流量统计错误
- 🛠️ fix:限速失败

</details>

## 架构

```
┌─────────────────────────────────────────────────────┐
│                    Master (miaomiaowux)              │
│                                                     │
│  /api/remote/ws        WebSocket 双向通信            │
│  /api/remote/traffic   HTTP 流量推送                 │
│  /api/remote/speed     HTTP 速度推送                 │
│  /api/remote/heartbeat HTTP 心跳                    │
└──────────────────────┬──────────────────────────────┘
                       │
          ┌────────────┼────────────┐
          │ WebSocket  │ HTTP Push  │ Pull (被动)
          └────────────┼────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│                    mmw-agent                         │
│                                                     │
│  内部组件:                                           │
│  ├── Agent Client    连接管理 + 数据上报              │
│  ├── Collector       Xray 流量采集 (/debug/vars)     │
│  ├── Handler         本地管理 API (/api/child/*)     │
│  └── xRPC            Xray gRPC 管理                  │
│                                                     │
│  监听端口: :23889 (管理API)                          │
│  Xray metrics: :38889 (/debug/vars)                 │
└─────────────────────────────────────────────────────┘
```

## 构建与运行

```bash
# 构建
go build -ldflags="-s -w" -o mmw-agent ./cmd/mmw-agent

# 运行（配置文件方式）
./mmw-agent -c config.yaml

# 运行（环境变量方式）
MMWX_MASTER_URL=https://master.example.com \
MMWX_MASTER_TOKEN=your-token \
./mmw-agent
```

## 配置

### 配置文件 (YAML)

```yaml
master_server: "https://master.example.com"
remote_token: "your-server-token"
connection_mode: "auto"          # auto | websocket | http | pull
listen_port: 23889
child_api_token: ""              # 可选，pull模式API认证
traffic_report_interval: 60      # 秒
speed_report_interval: 3         # 秒
heartbeat_interval: 30           # 秒
xray_servers:                    # 可选，不配则自动发现
  - config_path: "/usr/local/etc/xray/config.json"
```

### 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `MMWX_MASTER_URL` | 主控地址 | — |
| `MMWX_MASTER_TOKEN` | 服务器令牌 | — |
| `MMWX_CONNECTION_MODE` | 连接模式 | `auto` |
| `MMWX_LISTEN_PORT` | 监听端口 | `23889` |
| `MMWX_CHILD_API_TOKEN` | Pull API 认证令牌 | — |
| `MMWX_TRAFFIC_INTERVAL` | 流量上报间隔(秒) | `60` |
| `MMWX_SPEED_INTERVAL` | 速度上报间隔(秒) | `3` |
| `MMWX_HEARTBEAT_INTERVAL` | 心跳间隔(秒) | `30` |

环境变量优先级高于配置文件。

### Xray 自动发现

未显式配置 `xray_servers` 时，agent 按以下路径搜索 Xray 配置：

1. `/usr/local/etc/xray/config.json`
2. `/etc/xray/config.json`
3. `/opt/xray/config.json`

---

## 连接模式

### Auto（推荐）

自动回退链：WebSocket → HTTP → Pull，带指数退避重连。

```
尝试 WebSocket 连接
  ├── 成功 → 保持 WebSocket 双向通信
  │         断开后退避重连，期间通过 HTTP 发送流量保持在线
  └── 失败 → 尝试 HTTP 推送
                ├── 成功 → 定时 HTTP POST 上报
                │         连续 5 次失败后重试上层
                └── 失败 → 回退 Pull 模式
                              等待退避时间后重试上层
```

退避策略：基础 5s，指数增长，上限 5min。

### WebSocket

全双向通信。支持：
- Agent → Master：流量、速度、心跳、证书结果、扫描结果、域延迟探测结果
- Master → Agent：证书请求、证书部署、令牌更新、域延迟探测

连续 5 次连接失败后自动切换到 Auto 模式回退。

### HTTP

单向推送。Agent 定时 POST 数据到 Master：
- `/api/remote/traffic` — 流量数据
- `/api/remote/speed` — 速度数据
- `/api/remote/heartbeat` — 心跳

### Pull

被动模式。Agent 仅暴露本地 API，等待 Master 主动拉取：
- `GET /api/child/traffic`
- `GET /api/child/speed`

---

## 认证机制

所有请求必须同时满足：

1. **User-Agent**: `miaomiaowux/0.1`
2. **Authorization**: `Bearer <token>`（或 `X-Remote-Token` header）

未通过认证的连接被**静默丢弃**（TCP hijack 后直接关闭，不返回 HTTP 响应）。

---

## API 文档

### 健康检查（无需认证）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 健康检查 |

### 本地管理 API (`/api/child/*`)

以下所有端点均需通过 Silent Auth 中间件认证。

#### 服务管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/child/services/status` | 获取 Xray/Nginx 服务运行状态 |
| POST | `/api/child/services/control` | 控制服务启停 |

```json
// POST /api/child/services/control
{ "service": "xray", "action": "restart" }
// service: xray | nginx
// action: start | stop | restart
```

#### Xray 管理

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/child/xray/install` | 安装 Xray（SSE 流式输出） |
| POST | `/api/child/xray/remove` | 卸载 Xray（SSE 流式输出） |
| GET | `/api/child/xray/config` | 获取 Xray 完整配置 |
| PUT | `/api/child/xray/config` | 写入 Xray 完整配置 |
| GET | `/api/child/xray/config/files` | 列出 Xray 配置文件 |
| GET | `/api/child/xray/system-config` | 获取 Xray 系统配置路径信息 |

#### Nginx 管理

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/child/nginx/install` | 安装 Nginx（SSE 流式输出） |
| POST | `/api/child/nginx/remove` | 卸载 Nginx（SSE 流式输出） |
| GET | `/api/child/nginx/config` | 获取 Nginx 配置 |
| PUT | `/api/child/nginx/config` | 写入 Nginx 配置 |
| GET | `/api/child/nginx/config/files` | 列出 Nginx 配置文件 |

#### Xray 入站/出站/路由 (gRPC)

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/child/inbounds` | 列出所有入站 |
| POST | `/api/child/inbounds` | 添加入站 |
| PUT | `/api/child/inbounds` | 更新入站 |
| DELETE | `/api/child/inbounds` | 删除入站（query: `?tag=xxx`） |
| GET | `/api/child/outbounds` | 列出所有出站 |
| POST | `/api/child/outbounds` | 添加出站 |
| PUT | `/api/child/outbounds` | 更新出站 |
| DELETE | `/api/child/outbounds` | 删除出站（query: `?tag=xxx`） |
| GET | `/api/child/routing` | 获取路由规则 |
| PUT | `/api/child/routing` | 更新路由规则 |

请求体为标准 Xray JSON 配置对象。

#### 扫描与探测

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/child/scan` | 扫描 Xray 运行状态与入站配置 |
| POST | `/api/child/domains/latency` | 域名延迟探测（TCP） |

```json
// POST /api/child/domains/latency
{
  "domains": ["example.com", "test.org"],
  "timeout_ms": 5000
}
// 并发 TCP 探测，自动去重，最多 200 个域名
```

**扫描自动补全**：`/api/child/scan` 会检查 Xray 配置完整性，自动补全缺失的 `api`、`stats`、`policy` 段及 API 入站和路由规则，补全后自动重启 Xray。

#### 系统信息

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/child/system/info` | 获取系统信息（OS、CPU、内存、磁盘） |

#### Pull 模式数据接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/child/traffic` | 获取当前 Xray 流量统计 |
| GET | `/api/child/speed` | 获取当前系统网络速度 |

```json
// GET /api/child/traffic 响应
{
  "stats": {
    "inbound":  { "tag": { "uplink": 1024, "downlink": 2048 } },
    "outbound": { "tag": { "uplink": 512,  "downlink": 1024 } },
    "user":     { "email": { "uplink": 256, "downlink": 512 } }
  }
}

// GET /api/child/speed 响应
{
  "upload_speed": 102400,
  "download_speed": 204800
}
// 单位：字节/秒 (B/s)，来源：/proc/net/dev 物理网卡聚合
```

---

## WebSocket 交互协议

连接地址：`ws(s)://<master>/api/remote/ws`

### 消息格式

所有消息为 JSON，统一结构：

```json
{
  "type": "<message_type>",
  "payload": { ... }
}
```

### 连接流程

```
Agent                                    Master
  │                                        │
  │──── WebSocket Connect ────────────────>│
  │     Header: User-Agent: miaomiaowux/0.1
  │                                        │
  │──── auth ─────────────────────────────>│
  │     { "token": "server-token" }        │
  │                                        │
  │<─── auth_result ──────────────────────│
  │     { "success": true }                │
  │                                        │
  │──── traffic (定时) ───────────────────>│
  │──── speed (定时) ─────────────────────>│
  │──── heartbeat (定时) ─────────────────>│
  │                                        │
  │<─── cert_request ─────────────────────│
  │──── cert_update ──────────────────────>│
  │                                        │
  │<─── cert_deploy ──────────────────────│
  │<─── token_update ─────────────────────│
  │<─── domain_latency_probe ─────────────│
  │──── domain_latency_result ────────────>│
  │                                        │
  │──── scan_result (首次连接) ───────────>│
```

### 消息类型

#### Agent → Master

| type | 说明 | payload |
|------|------|---------|
| `auth` | 认证 | `{ "token": "string" }` |
| `traffic` | 流量数据 | `{ "stats": { "inbound": {}, "outbound": {}, "user": {} } }` |
| `speed` | 实时速度 | `{ "upload_speed": int64, "download_speed": int64 }` |
| `heartbeat` | 心跳 | `{ "boot_time": "RFC3339" }` |
| `cert_update` | 证书申请结果 | `{ "cert_id": int, "domain": "string", "success": bool, "cert_pem": "string", "key_pem": "string", "issue_date": "time", "expiry_date": "time", "error": "string" }` |
| `scan_result` | Xray 扫描结果 | `{ "xray_running": bool, "xray_version": "string", "inbounds": [...] }` |
| `domain_latency_result` | 域延迟探测结果 | `{ "request_id": "string", "success": bool, "results": [{ "domain": "string", "latency_ms": int64, "success": bool }] }` |

#### Master → Agent

| type | 说明 | payload |
|------|------|---------|
| `auth_result` | 认证结果 | `{ "success": bool, "message": "string" }` |
| `cert_request` | 请求申请证书 | `{ "cert_id": int, "domain": "string", "email": "string", "provider": "string", "challenge_mode": "string", ... }` |
| `cert_deploy` | 部署证书文件 | `{ "domain": "string", "cert_pem": "string", "key_pem": "string", "cert_path": "string", "key_path": "string", "reload": "nginx\|xray\|both\|none" }` |
| `token_update` | 推送新令牌 | `{ "server_token": "string", "expires_at": "RFC3339" }` |
| `domain_latency_probe` | 域延迟探测请求 | `{ "request_id": "string", "domains": ["string"], "timeout_ms": int }` |

### 认证流程

1. Agent 建立 WebSocket 连接（必须携带 `User-Agent: miaomiaowux/0.1`）
2. Agent 发送 `auth` 消息，携带服务器令牌
3. Master 验证令牌，返回 `auth_result`
4. 认证成功后进入消息循环；未认证的消息会收到 `auth_result { success: false, message: "Authentication required" }`

### 令牌轮换

Master 可通过 `token_update` 推送新令牌。Agent 收到后：
1. 将新令牌持久化到本地配置
2. 后续所有请求使用新令牌
3. Master 端同步更新连接映射

### 首次连接自动扫描

Agent 认证成功后自动执行 Xray 扫描，将结果通过 `scan_result` 发送给 Master。如果服务器处于 `pending` 状态且配置了域名和 443 端口，Master 会自动触发 steal-self 配置部署。

---

## 数据采集

### 流量采集

从 Xray 的 `/debug/vars` HTTP 端点采集，解析 `stats` 对象中的 inbound/outbound/user 流量数据。

metrics 地址从 Xray 配置文件的 `api` 段中的 `listen` 字段获取，默认 `127.0.0.1:38889`。

### 速度采集

读取 `/proc/net/dev`，聚合所有物理网卡（排除 `lo`）的收发字节数，计算两次采样间的速率差值。

```
上传速度 = (当前 TX - 上次 TX) / 采样间隔
下载速度 = (当前 RX - 上次 RX) / 采样间隔
```
