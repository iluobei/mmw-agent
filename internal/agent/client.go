package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"crypto/ed25519"
	"encoding/base64"

	"mmw-agent/internal/collector"
	"mmw-agent/internal/config"
	"mmw-agent/internal/constants"
	"mmw-agent/internal/discovery"
	"mmw-agent/internal/embedded"
	"mmw-agent/internal/limiter"
	"mmw-agent/internal/securechan"
	"mmw-agent/internal/util"
	"mmw-agent/internal/version"
	"mmw-agent/internal/xrayconf"
	"mmw-agent/internal/xrayctl"

	"github.com/gorilla/websocket"
)

// ConnectionMode 表示当前连接模式。
type ConnectionMode string

const (
	ModeWebSocket ConnectionMode = "websocket"
	ModeHTTP      ConnectionMode = "http"
	ModePull      ConnectionMode = "pull"
	ModeAuto      ConnectionMode = "auto"
)

// Client 表示连接主控端的 agent 客户端。
type Client struct {
	config      *config.Config
	collector   *collector.Collector
	xrayServers []config.XrayServer
	wsConn      *websocket.Conn
	wsMu        sync.Mutex
	connected   bool
	reconnects  int
	stopCh      chan struct{}
	wg          sync.WaitGroup

	startTime time.Time // 进程启动时间（固定不变，用于重启检测）

	// 连接状态
	currentMode   ConnectionMode
	httpClient    *http.Client
	httpAvailable bool
	modeMu        sync.RWMutex

	// 速率计算（基于系统网卡统计）
	lastRxBytes    int64
	lastTxBytes    int64
	lastSampleTime time.Time
	speedMu        sync.Mutex

	// 流量采集时间（用于计算用户网速）
	lastTrafficTime time.Time

	// 限速评估（基于快照，不重置计数器）
	lastLimitEvalTime   time.Time
	lastUserTrafficSnap map[string]int64

	// 嵌入模式
	embeddedXray *embedded.EmbeddedXray

	// 伪装探针「真数据」采集状态(主控通过 config_update 下发开关 + ping 目标)。
	// 只在总开关 + 各子开关开时采集对应项;probeMu 保护配置与最新 ping 结果。
	probeMu               sync.Mutex
	probeCollectCPU       bool
	probeCollectMem       bool
	probeCollectDisk      bool
	probeCollectPing      bool
	probePingTargets      []ProbePingTarget
	probePingIntMs        int                  // ping 间隔,默认 5000
	probeLatestPing       []ProbeLatencySample // ping goroutine 写、traffic tick 读
	probeLatestPingAt     int64                // 最近一轮 ping 的采样时刻
	probeLatestPingSentAt int64                // 已上报过的轮次时刻,用于去重(见 probeLatestPingSnapshot)
	probeSys              *sysMetricsCollector // CPU% 差分需跨采样状态,单个采集协程独占

	// 活跃的 traffic ticker 注册表,key 是 *time.Ticker。
	// 4 处 runXxxLoop (WebSocket / HTTP / Pull / Pull+TrafficReport) 起 ticker 时 Store,defer Delete。
	// handleConfigUpdate 拿到新 interval 时遍历 Reset → 实现热重载,无需重连 / 重启。
	trafficTickers sync.Map

	// 许可证状态
	licenseStatus *LicenseStatus
	licenseMu     sync.RWMutex

	// 加密通信
	masterPubKey  ed25519.PublicKey
	wsSession     *securechan.Session
	wsSessionMu   sync.Mutex
	httpSession   *securechan.Session
	httpSessionMu sync.Mutex

	// 公网 IPv4 / IPv6 缓存。后台 ipProbeLoop goroutine 持续 detect 直到拿到,
	// 心跳 / auth 直接读缓存(不阻塞)。ipMu 保护并发读写。
	ipMu       sync.RWMutex
	publicIPv4 string
	publicIPv6 string

	// 重连唤醒信号:IP 漂移 / 外部主动触发时,正在退避 backoff 的
	// waitWithTrafficReport / runPullModeWithTrafficReport 立即退出,
	// 让上层重新尝试 WS 连接 — 避免换 IP 后还呆在 5 分钟退避里。
	// buffered=1,triggerReconnect 用 non-blocking send,信号有就有,丢了也无所谓。
	reconnectSignal chan struct{}

	// 反向 RPC 路由表 — 跟普通 HTTP mux 共享同一份 /api/child/* handler 实例,
	// 区别仅在请求是从 WS 帧解出来构造的 *http.Request,而不是从 net.Listener 收来的。
	// nil = 主程序未注入 → handleRPCCall 直接报 503 错回 master,master 自动 fallback HTTP。
	rpcMux *http.ServeMux

	// warpStatusFn 返回 agent 当前 WARP 是否已注册。auth + heartbeat 时调用上报给 master,
	// 供 master 显示 server 卡片的 W 图标 badge。nil = 老 agent / 未注入 → 上报 false。
	warpStatusFn func() bool

	// 端口隐身钩子:WS 鉴权成功 → onWSConnected(主程序据此延迟关闭入站监听,隐藏端口);
	// WS 断开 → onWSDisconnected(主程序立即重开监听,保证主控 HTTP/pull 回退可达)。
	// nil = 未启用该特性 → 监听始终常开,行为同旧版。
	onWSConnected    func()
	onWSDisconnected func()

	// onXrayAuthChange 由 main.go 注入:主控下发的许可证配额授权变化时调用(true=授权→确保 xray 运行,
	// false=超额→停 xray)。Client 不直接持 ManageHandler 引用,故用回调解耦(仿 onWSConnected 范式)。
	onXrayAuthChange func(authorized bool)
}

// SetXrayAuthHandler 注入「许可证配额授权变化 → 停/启 xray」回调。main.go 启动时调一次。
func (c *Client) SetXrayAuthHandler(fn func(authorized bool)) {
	c.onXrayAuthChange = fn
}

// SetListenGateHooks 注入"端口隐身"钩子(见 onWSConnected / onWSDisconnected 字段)。
// main.go 仅在 cfg.HidePortOnWS 开启时调用。
func (c *Client) SetListenGateHooks(onUp, onDown func()) {
	c.onWSConnected = onUp
	c.onWSDisconnected = onDown
}

// SetRPCMux 由 main.go 在注册完 /api/child/* 路由后注入。共享 mux 让 WS RPC 路径完全复用现有 handler。
func (c *Client) SetRPCMux(mux *http.ServeMux) {
	c.rpcMux = mux
}

// SetWarpStatusFn 注入"查 WARP 是否已注册"的回调,auth / heartbeat 上报用。
func (c *Client) SetWarpStatusFn(fn func() bool) {
	c.warpStatusFn = fn
}

// getPublicIPv4 / getPublicIPv6 是 ipMu 保护的读取器,供 auth / heartbeat 调用。
func (c *Client) getPublicIPv4() string {
	c.ipMu.RLock()
	defer c.ipMu.RUnlock()
	return c.publicIPv4
}
func (c *Client) getPublicIPv6() string {
	c.ipMu.RLock()
	defer c.ipMu.RUnlock()
	return c.publicIPv6
}

// sameHostAsMaster 判断主控是否与本 agent 同机 —— 「反代主控」的硬前提(proxy_pass 127.0.0.1 才通)。
// 只做非阻塞判断:master_url host 是 localhost/127.0.0.1/::1,或等于本机已探测到的公网 IPv4/v6。
// 不 resolve 域名(避免阻塞心跳);宿主机反代主控的推荐配置是 master_url=http://127.0.0.1:<port>。
func (c *Client) sameHostAsMaster() bool {
	u, err := url.Parse(c.config.MasterURL)
	if err != nil {
		return false
	}
	host := u.Hostname()
	switch strings.ToLower(host) {
	case "":
		return false
	case "localhost", "127.0.0.1", "::1":
		return true
	}
	if v4 := c.getPublicIPv4(); v4 != "" && host == v4 {
		return true
	}
	if v6 := c.getPublicIPv6(); v6 != "" && host == v6 {
		return true
	}
	return false
}

// startIPProbeLoop 后台持续 detect 出口 v4 / v6,直到拿到才停。
// 启动时立即跑一次(给首次心跳留点时间);失败后每 30 秒重试,成功后退到 5 分钟一次轮询
// (兜底应对 NAT 出口 IP 漂移)。
//
// 跟旧版直接在 heartbeat 里同步 detect 的对比:
//   - 旧版:detect 阻塞心跳最多 12s,失败仍每 5 分钟阻塞一次
//   - 新版:心跳零阻塞,detect 异步;v4 失败时 30s 后台重试,远快于 5 分钟心跳
//
// 这是修复"重启后 v4 偶发探测失败 → 进程级永久空缓存"的核心机制。
func (c *Client) startIPProbeLoop(ctx context.Context) {
	go c.ipProbeLoop(ctx)
}

func (c *Client) ipProbeLoop(ctx context.Context) {
	probeOnce := func() {
		c.ipMu.RLock()
		curV4, curV6 := c.publicIPv4, c.publicIPv6
		c.ipMu.RUnlock()

		// 每次都重新 detect — 注释里说的"5min 轮询兜底 IP 漂移"必须真的做漂移检测,
		// 否则缓存一旦写上就永远不变,换 IP 后心跳上报的 public_ipv4 还是老值,
		// master 入库老 IP → 反向请求全失败 → master 看不到 agent 上线。
		// detect 失败(返回空)时保留旧值,避免临时探测失败把缓存清空。
		newV4 := c.detectPublicIPv4()
		if newV4 == "" {
			newV4 = curV4
		}
		newV6 := c.detectPublicIPv6()
		if newV6 == "" {
			newV6 = curV6
		}

		v4Changed := newV4 != curV4
		v6Changed := newV6 != curV6
		if v4Changed || v6Changed {
			c.ipMu.Lock()
			c.publicIPv4 = newV4
			c.publicIPv6 = newV6
			c.ipMu.Unlock()
			if v4Changed {
				log.Printf("[Agent] Public IPv4 changed: %q -> %q", curV4, newV4)
			}
			if v6Changed {
				log.Printf("[Agent] Public IPv6 changed: %q -> %q", curV6, newV6)
			}
			// IP 变化 → 立刻唤醒 backoff,让 agent 用新 IP 重连(避免换 IP 后还在 5 分钟退避里睡)。
			// 首次探到 v4 也走这条 — curV4="" → newV4!="" 是 changed,正好让冷启动 backoff 也加速。
			c.triggerReconnect()
		}
	}

	// 启动后立即 detect 一次,让首次心跳有更高概率带上 v4
	probeOnce()

	// 重试节奏:v4 还没拿到 → 30s 重试;已拿到 → 1min 轮询(原来 5min,换 IP 场景反应太慢)
	for {
		c.ipMu.RLock()
		needV4 := c.publicIPv4 == ""
		c.ipMu.RUnlock()

		delay := 1 * time.Minute
		if needV4 {
			delay = 30 * time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		probeOnce()
	}
}

// 创建 agent 客户端。
func NewClient(cfg *config.Config) *Client {
	c := &Client{
		config:          cfg,
		collector:       collector.NewCollector(),
		xrayServers:     cfg.XrayServers,
		stopCh:          make(chan struct{}),
		startTime:       time.Now(),
		currentMode:     ModePull,
		reconnectSignal: make(chan struct{}, 1),
	}
	// httpClient 用跟 WS 同款的 v4 优先 dialer — 否则 Go 默认 dialer 在 dual-stack 主机上
	// Happy Eyeballs 偏好 v6,HTTP heartbeat 全程走 v6 → master 看 RemoteAddr 是 v6 →
	// db.ip_address 误写 v6 → IPv4-only master 反向请求 502。
	c.httpClient = &http.Client{
		Timeout: constants.DefaultHTTPClientTimeout,
		Transport: &http.Transport{
			DialContext:           c.preferV4DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			IdleConnTimeout:       90 * time.Second,
		},
	}
	if cfg.MasterPublicKey != "" {
		if pub, err := securechan.ParsePublicKey(cfg.MasterPublicKey); err == nil {
			c.masterPubKey = pub
			log.Printf("[Agent] Master public key loaded, encrypted communication enabled")
		} else {
			log.Printf("[Agent] Warning: invalid master_public_key: %v", err)
		}
	}
	return c
}

// 生成 WebSocket 握手请求头。
func (c *Client) wsHeaders() http.Header {
	h := http.Header{}
	h.Set(constants.HeaderUserAgent, constants.AgentUserAgent)
	return h
}

// 创建带标准请求头的 HTTP 请求。
func (c *Client) newRequest(ctx context.Context, method, urlStr string, body []byte) (*http.Request, error) {
	var req *http.Request
	var err error
	if body != nil {
		req, err = http.NewRequestWithContext(ctx, method, urlStr, bytes.NewReader(body))
	} else {
		req, err = http.NewRequestWithContext(ctx, method, urlStr, nil)
	}
	if err != nil {
		return nil, err
	}
	req.Header.Set(constants.HeaderContentType, constants.ContentTypeJSON)
	req.Header.Set(constants.HeaderAuthorization, constants.BearerPrefix+c.config.Token)
	req.Header.Set(constants.HeaderUserAgent, constants.AgentUserAgent)
	return req, nil
}

// 按配置启动客户端。
func (c *Client) Start(ctx context.Context) {
	log.Printf("[Agent] Starting in %s mode", c.config.ConnectionMode)

	// 后台 IP detect 循环 — 启动时立即跑一次,失败时 30s 重试,直到拿到 v4 / v6。
	// 不阻塞 WS / HTTP / pull 模式的握手路径。
	c.startIPProbeLoop(ctx)

	// 伪装探针 ping 循环(独立于 traffic tick)。未开启 ping 时循环空转,开销可忽略。
	go c.runProbePingLoop()

	mode := ConnectionMode(c.config.ConnectionMode)

	switch mode {
	case ModeWebSocket:
		c.wg.Add(1)
		go c.runWebSocket(ctx)

	case ModeHTTP:
		c.wg.Add(1)
		go c.runHTTPReporter(ctx)

	case ModePull:
		c.setCurrentMode(ModePull)
		log.Printf("[Agent] Pull mode enabled - API will be served at /api/child/traffic and /api/child/speed")
		// 启动后先通过 HTTP 上报一次心跳信息
		if err := c.sendHeartbeatHTTP(ctx); err != nil {
			log.Printf("[Agent] Failed to send initial heartbeat in pull mode: %v", err)
		}

	case ModeAuto:
		fallthrough
	default:
		c.wg.Add(1)
		go c.runAutoMode(ctx)
	}
}

// triggerReconnect 唤醒正在 backoff 退避里睡的 goroutine 立刻重连。
// non-blocking:信道满了说明已有挂起信号,丢这次也行。
//
// 顺手把 reconnects 清零:这是"外部触发的重连机会"(典型 IP 漂移),
// 不应该被旧的累积失败 backoff(可能已经到 5 分钟上限)拖累 — 新 IP 网络条件
// 是全新的,即使第一次失败也应该从最短 backoff (5s) 重新开始而不是 300s。
func (c *Client) triggerReconnect() {
	c.reconnects = 0
	select {
	case c.reconnectSignal <- struct{}{}:
	default:
	}
}

// 停止客户端。
//
// 关键顺序:**先关 wsConn**,让所有正在 ReadMessage/WriteMessage 阻塞的 goroutine
// 立刻返回 error → 它们的 outer loop 看见 ctx 已 cancel → return → wg.Done。
// 否则 WriteMessage 没 WriteDeadline 时会卡到 TCP retransmission(~15 分钟),
// wg.Wait 永远等不到,systemctl restart 在用户视角就是"假死"。
// 即便如此,wg.Wait 仍用 AgentStopTimeout 兜底 — 任何意外卡死也不让 systemd 等到 90s TimeoutStopSec。
func (c *Client) Stop() {
	close(c.stopCh)

	c.wsMu.Lock()
	if c.wsConn != nil {
		c.wsConn.Close()
		c.wsConn = nil
	}
	c.wsMu.Unlock()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		log.Printf("[Agent] Stopped")
	case <-time.After(constants.AgentStopTimeout):
		log.Printf("[Agent] Stop timed out after %s, exiting with leaked goroutines", constants.AgentStopTimeout)
	}
}

// 返回 WebSocket 连接状态。
func (c *Client) IsConnected() bool {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	return c.connected
}

// 返回当前连接模式。
func (c *Client) GetCurrentMode() ConnectionMode {
	c.modeMu.RLock()
	defer c.modeMu.RUnlock()
	return c.currentMode
}

// 设置当前连接模式。
func (c *Client) setCurrentMode(mode ConnectionMode) {
	c.modeMu.Lock()
	defer c.modeMu.Unlock()
	c.currentMode = mode
}

// 维护 WebSocket 连接，并在失败时回退自动模式。
func (c *Client) runWebSocket(ctx context.Context) {
	defer c.wg.Done()

	maxConsecutiveFailures := constants.WebSocketMaxConsecutiveFailures
	maxAuthFailures := constants.WebSocketMaxAuthFailures
	consecutiveFailures := 0
	authFailures := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		c.setCurrentMode(ModeWebSocket)
		if err := c.connectAndRun(ctx); err != nil {
			if ctx.Err() != nil {
				log.Printf("[Agent] Context canceled, stopping gracefully")
				return
			}

			// 判断是否为鉴权错误
			if authErr, ok := err.(*AuthError); ok {
				authFailures++
				if authErr.IsTokenInvalid() {
					log.Printf("[Agent] Authentication failed (invalid token): %v", err)
					if authFailures >= maxAuthFailures {
						log.Printf("[Agent] Too many auth failures (%d), entering sleep mode (30 min backoff)", authFailures)
						c.waitWithTrafficReport(ctx, constants.AuthFailureSleepBackoff)
						authFailures = 0
						continue
					}
				}
				// 鉴权错误使用更长退避时间
				backoff := time.Duration(authFailures) * constants.AuthFailureBackoffStep
				if backoff > constants.AuthFailureMaxBackoff {
					backoff = constants.AuthFailureMaxBackoff
				}
				log.Printf("[Agent] Auth error, reconnecting in %v...", backoff)
				c.waitWithTrafficReport(ctx, backoff)
				continue
			}

			log.Printf("[Agent] WebSocket error: %v", err)
			consecutiveFailures++
			authFailures = 0 // Reset auth failures on non-auth errors

			if consecutiveFailures >= maxConsecutiveFailures {
				log.Printf("[Agent] Too many WebSocket failures (%d), switching to auto mode for fallback...", consecutiveFailures)
				c.runAutoModeLoop(ctx)
				consecutiveFailures = 0
				continue
			}
		} else {
			consecutiveFailures = 0
			authFailures = 0
		}

		backoff := c.calculateBackoff()
		log.Printf("[Agent] Reconnecting in %v...", backoff)
		c.waitWithTrafficReport(ctx, backoff)
	}
}

// 计算重连退避时长。
func (c *Client) calculateBackoff() time.Duration {
	c.reconnects++
	// 指数退避: 5s, 10s, 20s, 40s, 80s, 160s, 300s(上限)
	backoff := constants.ReconnectBaseBackoff
	for i := 1; i < c.reconnects && backoff < constants.ReconnectMaxBackoff; i++ {
		backoff *= 2
	}
	if backoff > constants.ReconnectMaxBackoff {
		backoff = constants.ReconnectMaxBackoff
	}
	return backoff
}

// 建立并维持 WebSocket 连接。
func (c *Client) connectAndRun(ctx context.Context) error {
	masterURL := c.config.MasterURL
	u, err := url.Parse(masterURL)
	if err != nil {
		return err
	}

	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}

	u.Path = constants.PathRemoteWebSocket

	log.Printf("[Agent] Connecting to %s", u.String())

	// WS 拨号 IPv4 优先,失败回退 IPv6 — 兼顾 IPv6-only 服务器。
	// 历史问题:agent 上 IPv6 路由可用时 getaddrinfo 默认偏好 IPv6,WS 源 IP 变 v6,
	// master 心跳兜底没拿到 PublicIPv4 时用源 IP 存进 db → IPv4-only 主控做 HTTP 反向请求
	// 全 502。优先 v4 解决 dual-stack 主机的偏好问题,失败 v6 兜底保住纯 v6 服务器。
	dialer := websocket.Dialer{
		HandshakeTimeout: constants.WebSocketHandshakeTimeout,
		NetDialContext:   c.preferV4DialContext,
	}

	conn, _, err := dialer.DialContext(ctx, u.String(), c.wsHeaders())
	if err != nil {
		return err
	}

	c.wsMu.Lock()
	c.wsConn = conn
	c.wsMu.Unlock()

	defer func() {
		c.wsMu.Lock()
		c.wsConn = nil
		c.connected = false
		c.wsMu.Unlock()
		c.wsSessionMu.Lock()
		c.wsSession = nil
		c.wsSessionMu.Unlock()
		conn.Close()
		// WS 断开:立即重开入站监听,保证主控 HTTP/pull 回退可达(早于重连退避)。
		if c.onWSDisconnected != nil {
			c.onWSDisconnected()
		}
	}()

	// 密钥交换（如果配置了 master 公钥）
	if c.masterPubKey != nil {
		session, err := c.performKeyExchange(conn)
		if err != nil {
			return fmt.Errorf("key exchange failed: %w", err)
		}
		c.wsSessionMu.Lock()
		c.wsSession = session
		c.wsSessionMu.Unlock()
		log.Printf("[Agent] Encrypted session established")
	}

	if err := c.authenticate(conn); err != nil {
		return err
	}

	c.wsMu.Lock()
	c.connected = true
	c.reconnects = 0
	c.wsMu.Unlock()

	log.Printf("[Agent] Connected and authenticated")

	// WS 鉴权成功:主控指令将走 WS 反向 RPC,入站端口不再需要 → 通知主程序(延迟)关闭监听。
	if c.onWSConnected != nil {
		c.onWSConnected()
	}

	// 连接成功后立即上报 agent 信息（listen_port）
	if err := c.sendHeartbeat(conn); err != nil {
		log.Printf("[Agent] Failed to send initial heartbeat: %v", err)
	}

	// 异步上报扫描结果，供主控端自动同步
	go c.sendScanResult(conn)

	return c.runMessageLoop(ctx, conn)
}

// 发送鉴权消息。
func (c *Client) authenticate(conn *websocket.Conn) error {
	// auth 时把缓存的 public_ipv4 也带上,让 master 在第一次 heartbeat 到来之前(~10s 窗口)
	// 就有正确的 v4 IP 可用,避开 master 重启 → preferV4DialContext 偶尔 v6 → auth 时 master
	// 用 WS 源 IP (v6) 写 db → master 反向请求 agent 走 v6 → 失败 的时序问题。
	// 直接读后台 ipProbeLoop 维护的缓存,不在心跳路径上同步 detect(否则阻塞 12s)。
	// 启动时 startIPProbeLoop 立即跑一次 detect,等首次心跳触发时通常已有值;
	// 偶发首次仍空也没关系,master 端 COALESCE 保留 db 旧 v4。
	publicIPv4 := c.getPublicIPv4()
	publicIPv6 := c.getPublicIPv6()
	// capabilities.rpc = true 告诉 master 本 agent 能接 WSMsgTypeRPCCall,master 可以把
	// 原本走 HTTP /api/child/* 的反向调用切到 WS,绕开 IPv6 漂移 / NAT 反向不通的痛点。
	// capabilities.stream = true 告诉 master 本 agent 还能跑 rpc_call(Stream:true) → rpc_stream_data
	// 流式协议,可替代 /api/child/xxx-stream SSE。两者实际依赖同一份 rpcMux,所以一起 toggle。
	rpcAvailable := c.rpcMux != nil
	warpInstalled := false
	if c.warpStatusFn != nil {
		warpInstalled = c.warpStatusFn()
	}
	authPayload, _ := json.Marshal(map[string]any{
		"token":               c.config.Token,
		"public_ipv4":         publicIPv4,
		"public_ipv6":         publicIPv6,
		"warp_installed":      warpInstalled,
		"same_host_as_master": c.sameHostAsMaster(), // 主控同机 → 前端可显示「反代主控」入口
		"agent_version":       version.Version,      // 主控经 WS auth 直接拿版本,不再反向 HTTP 拉 /api/child/system/info(端口隐身后仍可显示)
		"xray_mode":           c.config.XrayMode,    // 上报当前运行模式,主控据此校正 embedded→external 漂移(license 恢复后自动拉回 embedded)
		"capabilities": map[string]bool{
			"rpc":    rpcAvailable,
			"stream": rpcAvailable,
		},
	})

	msg := map[string]interface{}{
		"type":    "auth",
		"payload": json.RawMessage(authPayload),
	}

	if err := c.writeEncrypted(conn, msg); err != nil {
		return err
	}

	conn.SetReadDeadline(time.Now().Add(constants.WebSocketReadDeadline))
	_, message, err := conn.ReadMessage()
	if err != nil {
		return err
	}
	message = c.decryptMessage(message)

	var result struct {
		Type    string `json:"type"`
		Payload struct {
			Success bool   `json:"success"`
			Message string `json:"message"`
		} `json:"payload"`
	}

	if err := json.Unmarshal(message, &result); err != nil {
		return err
	}

	if result.Type != "auth_result" || !result.Payload.Success {
		return &AuthError{Message: result.Payload.Message}
	}

	return nil
}

// 执行 WS 密钥交换。
func (c *Client) performKeyExchange(conn *websocket.Conn) (*securechan.Session, error) {
	agentPriv, agentPub, err := securechan.GenerateEphemeral()
	if err != nil {
		return nil, err
	}

	kxPayload, _ := json.Marshal(map[string]string{
		"agent_ephemeral_pub": base64.StdEncoding.EncodeToString(agentPub),
	})
	msg := map[string]interface{}{
		"type":    "key_exchange",
		"payload": json.RawMessage(kxPayload),
	}
	_ = conn.SetWriteDeadline(time.Now().Add(constants.WebSocketWriteDeadline))
	if err := conn.WriteJSON(msg); err != nil {
		return nil, err
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, message, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}

	var resp struct {
		Type    string `json:"type"`
		Payload struct {
			MasterEphemeralPub string `json:"master_ephemeral_pub"`
			Signature          string `json:"signature"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(message, &resp); err != nil {
		return nil, err
	}
	if resp.Type != "key_exchange_resp" {
		return nil, fmt.Errorf("unexpected response type: %s", resp.Type)
	}

	masterEphPub, err := base64.StdEncoding.DecodeString(resp.Payload.MasterEphemeralPub)
	if err != nil || len(masterEphPub) != 32 {
		return nil, fmt.Errorf("invalid master ephemeral key")
	}

	sig, err := base64.StdEncoding.DecodeString(resp.Payload.Signature)
	if err != nil {
		return nil, fmt.Errorf("invalid signature encoding")
	}
	if !securechan.Verify(c.masterPubKey, masterEphPub, sig) {
		return nil, fmt.Errorf("master signature verification failed")
	}

	sharedSecret, err := securechan.ComputeSharedSecret(agentPriv, masterEphPub)
	if err != nil {
		return nil, err
	}

	return securechan.DeriveSession(sharedSecret, agentPub, masterEphPub, false)
}

// 加密写入 WS 消息。
func (c *Client) writeEncrypted(conn *websocket.Conn, msg interface{}) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	c.wsSessionMu.Lock()
	session := c.wsSession
	c.wsSessionMu.Unlock()

	if session != nil {
		envelope, err := session.Encrypt(data)
		if err != nil {
			return err
		}
		c.wsMu.Lock()
		// SetWriteDeadline 必须设 — 没设这一行,TCP send buffer 满时
		// WriteMessage 会卡到 TCP retransmission(~15min) → 拖死 runMessageLoop。
		_ = conn.SetWriteDeadline(time.Now().Add(constants.WebSocketWriteDeadline))
		err = conn.WriteMessage(websocket.BinaryMessage, envelope)
		c.wsMu.Unlock()
		return err
	}

	c.wsMu.Lock()
	_ = conn.SetWriteDeadline(time.Now().Add(constants.WebSocketWriteDeadline))
	err = conn.WriteMessage(websocket.TextMessage, data)
	c.wsMu.Unlock()
	return err
}

// 解密读取 WS 消息。
func (c *Client) decryptMessage(message []byte) []byte {
	c.wsSessionMu.Lock()
	session := c.wsSession
	c.wsSessionMu.Unlock()

	if session != nil && len(message) > 0 && message[0] == securechan.EnvelopeVersion {
		if plaintext, err := session.Decrypt(message); err == nil {
			return plaintext
		} else {
			log.Printf("[Agent] Decrypt error: %v", err)
		}
	}
	return message
}

// 处理流量、速率和心跳上报。
func (c *Client) runMessageLoop(ctx context.Context, conn *websocket.Conn) error {
	trafficTicker := time.NewTicker(c.config.TrafficReportInterval)
	defer c.registerTrafficTicker(trafficTicker)() // master push config_update 时 Reset 该 ticker
	speedTicker := time.NewTicker(c.config.SpeedReportInterval)
	heartbeatTicker := time.NewTicker(constants.WebSocketHeartbeatInterval)
	defer trafficTicker.Stop()
	defer speedTicker.Stop()
	defer heartbeatTicker.Stop()

	msgCh := make(chan []byte, 10)
	errCh := make(chan error, 1)
	go func() {
		for {
			conn.SetReadDeadline(time.Now().Add(constants.WebSocketIdleDeadline))
			_, message, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			message = c.decryptMessage(message)
			select {
			case msgCh <- message:
			default:
				log.Printf("[Agent] Message queue full, dropping message")
			}
		}
	}()

	c.sendTrafficData(conn)
	c.sendSpeedData(conn)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopCh:
			return nil
		case err := <-errCh:
			return err
		case msg := <-msgCh:
			c.handleMessage(conn, msg)
		case <-trafficTicker.C:
			if err := c.sendTrafficData(conn); err != nil {
				return err
			}
		case <-speedTicker.C:
			if err := c.sendSpeedData(conn); err != nil {
				return err
			}
			c.evaluateSpeedLimits()
		case <-heartbeatTicker.C:
			if err := c.sendHeartbeat(conn); err != nil {
				return err
			}
		}
	}
}

// 采集并发送流量数据。
func (c *Client) sendTrafficData(conn *websocket.Conn) error {
	now := time.Now()
	stats, err := c.collectLocalMetrics()
	if err != nil {
		log.Printf("[Agent] Failed to collect metrics: %v", err)
		stats = &collector.XrayStats{}
	}

	// collectLocalMetrics 使用 Set(0) 重置了计数器，需要同步重置快照基线
	c.lastUserTrafficSnap = nil
	c.lastLimitEvalTime = now

	payloadMap := map[string]interface{}{
		"stats": stats,
	}

	// 系统级网卡累计 RX/TX —— 主控按 server.traffic_source='system' 时改用这两个值替代 xray 流量,
	// 跟 VPS 服务商面板的"网卡流量"口径对齐。boot_time_unix 给主控做 reboot 判定的辅助:同一 boot
	// 周期内 rx/tx 是单调累加,跨 boot 才会归零 → 主控据此区分"agent 暂时丢状态"vs"系统真重启"。
	sysRx, sysTx := c.getSystemNetworkStats()
	payloadMap["system"] = map[string]int64{
		"rx_total":       sysRx,
		"tx_total":       sysTx,
		"boot_time_unix": c.startTime.Unix(),
	}

	if c.embeddedXray != nil {
		onlineUsers := c.collectOnlineUsers()
		if len(onlineUsers) > 0 {
			payloadMap["online_users"] = onlineUsers
		}

		// 每 group 当前并发连接数(group="<user>|<物理节点ID>"),供主控聚合、用户视图展示。
		if l := c.embeddedXray.GetLimiter(); l != nil {
			if cc := l.ConnCountSnapshot(); len(cc) > 0 {
				payloadMap["conn_counts"] = cc
			}
		}

		if monitor := c.embeddedXray.GetSpeedMonitor(); monitor != nil && !c.lastTrafficTime.IsZero() {
			elapsed := now.Sub(c.lastTrafficTime)
			userDeltas := make(map[string]int64, len(stats.User))
			for email, td := range stats.User {
				userDeltas[email] = td.Uplink + td.Downlink
			}
			monitor.Evaluate(userDeltas, elapsed)
			if speeds := monitor.GetUserSpeeds(); len(speeds) > 0 {
				payloadMap["user_speeds"] = speeds
			}
		}
	}
	c.lastTrafficTime = now

	// 伪装探针真数据:按开关采系统指标 + 带上最近一轮 ping 结果。全 omitempty 语义——
	// 未开启任何项时不加字段,主控/旧主控都能安全忽略。
	if c.probeAnyEnabled() {
		if sm := c.collectProbeSysMetrics(); sm != nil {
			payloadMap["sysmetrics"] = sm
		}
		if lat := c.probeLatestPingSnapshot(); len(lat) > 0 {
			payloadMap["latency"] = lat
		}
	}

	payload, _ := json.Marshal(payloadMap)

	msg := map[string]interface{}{
		"type":    "traffic",
		"payload": json.RawMessage(payload),
	}

	if err := c.writeEncrypted(conn, msg); err != nil {
		return err
	}

	debugLogf("[Agent] Sent traffic data: %d inbounds, %d outbounds, %d users",
		len(stats.Inbound), len(stats.Outbound), len(stats.User))

	return nil
}

// 发送心跳消息。
func (c *Client) sendHeartbeat(conn *websocket.Conn) error {
	// 同 authenticate:从 ipProbeLoop 维护的缓存读,不阻塞心跳。
	publicIPv4 := c.getPublicIPv4()
	publicIPv6 := c.getPublicIPv6()
	listenPort, _ := strconv.Atoi(c.config.ListenPort)
	warpInstalled := false
	if c.warpStatusFn != nil {
		warpInstalled = c.warpStatusFn()
	}
	payload, _ := json.Marshal(map[string]any{
		"boot_time":           c.startTime,
		"listen_port":         listenPort,
		"local_time":          time.Now().Unix(),
		"public_ipv4":         publicIPv4,
		"public_ipv6":         publicIPv6,
		"warp_installed":      warpInstalled,
		"same_host_as_master": c.sameHostAsMaster(),
	})

	msg := map[string]interface{}{
		"type":    "heartbeat",
		"payload": json.RawMessage(payload),
	}

	return c.writeEncrypted(conn, msg)
}

// preferV4DialContext 是 v4 优先 v6 兜底的 dialer。给 WS / HTTP 拨号 master 用。
// 默认 net.Dialer 在 dual-stack 主机上优先 IPv6(getaddrinfo+Happy Eyeballs),导致连接源 IP
// 是 v6,master 写 db 也是 v6 → IPv4-only 主控反向 HTTP 请求 agent 全部 502。
//
// 行为分两档,由后台 ipProbeLoop 拿到的 publicIPv4 缓存决定:
//
//  1. ipProbeLoop 已确认本机 v4 出口可用(`getPublicIPv4() != ""`):
//     **严格 tcp4**,不 fallback v6。本机 v4 都通了,master 极大概率也接得到 v4 入站;
//     这一档把 v4 timeout 拉到 10s,让弱网 / 跨地域 TLS 握手有充裕窗口,避免被边缘 timeout
//     踹去走 v6,造成"v4 实际通但偶发慢一点就降级 v6 长锁"的现网症状。
//
//  2. v4 尚未探到(`getPublicIPv4() == ""`):
//     按老逻辑 tcp4 10s 优先,失败 tcp6 6s 兜底 — 兼顾刚启动还没 probe 完的窗口期,
//     和真正 IPv6-only 的 agent 服务器。
//
// 改成 method 是为了能读 c.getPublicIPv4();httpClient 的 Transport 也用这个函数 →
// 所有出向连接(WS + HTTP heartbeat + traffic + speed)统一行为,不再有"WS 走偏好但 HTTP
// 走 Go 默认 Happy Eyeballs"的口子。
func (c *Client) preferV4DialContext(ctx context.Context, _, addr string) (net.Conn, error) {
	v4 := &net.Dialer{Timeout: 10 * time.Second}
	if c.getPublicIPv4() != "" {
		// 已经知道本机 v4 出口通 → 严格 v4,不再 fallback v6
		return v4.DialContext(ctx, "tcp4", addr)
	}
	if conn, err := v4.DialContext(ctx, "tcp4", addr); err == nil {
		return conn, nil
	}
	v6 := &net.Dialer{Timeout: 6 * time.Second}
	return v6.DialContext(ctx, "tcp6", addr)
}

// detectPublicIPv4 严格探测本机公网 IPv4。**只**返回 v4 字符串,失败返回空串。
//
// 历史:曾经在 v4 探测失败时 fallback 到 v6,设计意图是兼顾 IPv6-only 服务器。但实际后果是:
// dual-stack 服务器若遇到 v4 endpoint 偶发不通(4.ipw.cn 慢 / DNS 抖动 / probe 服务 5xx),
// fallback 会把 v6 字符串装进 publicIPv4 缓存,master 用它覆盖 db.ip_address →
// IPv4-only master 反向 HTTP 全部 502。
//
// 新策略:v4 字段严格只装 v4。IPv6-only 服务器走 detectPublicIPv6() + master 端 IPAddressV6 候选。
// master `buildAgentURLCandidates` 已经能在 IPAddress=空 时只走 IPAddressV6,行为兼容。
func (c *Client) detectPublicIPv4() string {
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	client := &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
				return dialer.DialContext(ctx, "tcp4", addr)
			},
			TLSHandshakeTimeout: 3 * time.Second,
		},
	}
	defer client.CloseIdleConnections() // 用完关连接池,避免每 30-60s 新建 Transport 的瞬时连接/goroutine 堆积
	urls := []string{
		"https://api4.ipify.org",
		"https://ipv4.icanhazip.com",
		"https://v4.ident.me",
	}
	for _, u := range urls {
		resp, err := client.Get(u)
		if err != nil {
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}
		ip := strings.TrimSpace(string(body))
		parsed := net.ParseIP(ip)
		// 严格校验:必须是 v4。parsed.To4() != nil 同时排除 nil 和 v6 字符串。
		if parsed == nil || parsed.To4() == nil {
			continue
		}
		return ip
	}
	return ""
}

// detectPublicIPv6 仅探测公网 IPv6。dual-stack 服务器才会拿到非空,IPv4-only 返回空。
// 与 detectPublicIPv4 不同:这里强制 tcp6 + want6=true 校验,只接受 v6 字符串。
// 用途:auth/heartbeat 上报给 master,让 master HTTP 反向请求在 v4 失败时 fallback 试 v6。
func (c *Client) detectPublicIPv6() string {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
				return dialer.DialContext(ctx, "tcp6", addr)
			},
			TLSHandshakeTimeout: 5 * time.Second,
		},
	}
	defer client.CloseIdleConnections() // 用完关连接池,避免每 30-60s 新建 Transport 的瞬时连接/goroutine 堆积
	for _, u := range []string{
		"https://6.ipw.cn",
		"https://api6.ipify.org",
		"https://ipv6.icanhazip.com",
		"https://v6.ident.me",
	} {
		resp, err := client.Get(u)
		if err != nil {
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}
		ip := strings.TrimSpace(string(body))
		parsed := net.ParseIP(ip)
		if parsed == nil {
			continue
		}
		// 严格校验:必须是 v6(.To4() == nil 等同于 v6 literal)
		if parsed.To4() != nil {
			continue
		}
		return ip
	}
	return ""
}

// SetEmbeddedXray 设置嵌入模式的 Xray 实例。
func (c *Client) SetEmbeddedXray(ex *embedded.EmbeddedXray) {
	c.embeddedXray = ex
	log.Printf("[Agent] Embedded Xray reference updated (non-nil=%v)", ex != nil)
}

// GetEmbeddedXray 返回嵌入模式的 Xray 实例。
func (c *Client) GetEmbeddedXray() *embedded.EmbeddedXray {
	return c.embeddedXray
}

// 采集本机 Xray 流量指标。
func (c *Client) collectLocalMetrics() (*collector.XrayStats, error) {
	// 嵌入模式：直接从 stats.Manager 读取
	if c.embeddedXray != nil {
		stats := c.embeddedXray.CollectStats()
		if stats != nil {
			return stats, nil
		}
		return &collector.XrayStats{
			Inbound:  make(map[string]collector.TrafficData),
			Outbound: make(map[string]collector.TrafficData),
			User:     make(map[string]collector.TrafficData),
		}, nil
	}

	// 外部模式：通过 HTTP /debug/vars 拉取
	stats := &collector.XrayStats{
		Inbound:  make(map[string]collector.TrafficData),
		Outbound: make(map[string]collector.TrafficData),
		User:     make(map[string]collector.TrafficData),
	}

	for _, server := range c.xrayServers {
		host, port, err := c.collector.GetMetricsPortFromConfig(server.ConfigPath)
		if err != nil {
			log.Printf("[Agent] Failed to get metrics config for %s: %v", server.Name, err)
			continue
		}

		metrics, err := c.collector.FetchMetrics(host, port)
		if err != nil {
			log.Printf("[Agent] Failed to fetch metrics for %s: %v", server.Name, err)
			continue
		}

		if metrics.Stats != nil {
			collector.MergeStats(stats, metrics.Stats)
		}
	}

	return stats, nil
}

// 返回当前流量统计（拉取模式）。
func (c *Client) GetStats() (*collector.XrayStats, error) {
	return c.collectLocalMetrics()
}

// 返回当前速率（拉取模式）。
func (c *Client) GetSpeed() (uploadSpeed, downloadSpeed int64) {
	return c.collectSpeed()
}

// 使用三层回退：WebSocket -> HTTP -> Pull。
func (c *Client) runAutoMode(ctx context.Context) {
	defer c.wg.Done()
	c.runAutoModeLoop(ctx)
}

// 是自动模式的内部循环。
func (c *Client) runAutoModeLoop(ctx context.Context) {
	autoRetries := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		log.Printf("[Agent] Trying WebSocket connection...")
		if err := c.tryWebSocketOnce(ctx); err == nil {
			c.setCurrentMode(ModeWebSocket)
			log.Printf("[Agent] WebSocket mode active")
			if err := c.connectAndRun(ctx); err != nil {
				if ctx.Err() != nil {
					log.Printf("[Agent] Context canceled, stopping gracefully")
					return
				}
				log.Printf("[Agent] WebSocket disconnected: %v", err)
			}
			c.reconnects = 0
			autoRetries = 0
			continue
		} else {
			log.Printf("[Agent] WebSocket failed: %v, trying HTTP...", err)
		}

		if c.tryHTTPOnce(ctx) {
			c.setCurrentMode(ModeHTTP)
			log.Printf("[Agent] HTTP mode active")
			c.runHTTPReporterLoop(ctx)
			if ctx.Err() != nil {
				return
			}
			autoRetries = 0
			continue
		}

		c.setCurrentMode(ModePull)
		log.Printf("[Agent] Falling back to pull mode - API available at /api/child/traffic and /api/child/speed")
		c.sendHeartbeatHTTP(ctx)

		// 拉取模式退避: 30s, 60s, 120s, 240s, 300s(上限)
		autoRetries++
		pullDuration := constants.AutoModePullFallbackBackoff
		for i := 1; i < autoRetries && pullDuration < constants.ReconnectMaxBackoff; i++ {
			pullDuration *= 2
		}
		if pullDuration > constants.ReconnectMaxBackoff {
			pullDuration = constants.ReconnectMaxBackoff
		}

		c.runPullModeWithTrafficReport(ctx, pullDuration)

		if ctx.Err() != nil {
			return
		}
		log.Printf("[Agent] Retrying higher-priority connection modes...")
	}
}

// 执行一次 WebSocket 可用性探测。
func (c *Client) tryWebSocketOnce(ctx context.Context) error {
	masterURL := c.config.MasterURL
	u, err := url.Parse(masterURL)
	if err != nil {
		return err
	}

	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}
	u.Path = constants.PathRemoteWebSocket

	// 同 connectAndRun:探测拨号也走 v4 优先 v6 兜底,保持探测和正式连接同一选择
	dialer := websocket.Dialer{
		HandshakeTimeout: constants.WebSocketHandshakeTimeout,
		NetDialContext:   c.preferV4DialContext,
	}

	conn, _, err := dialer.DialContext(ctx, u.String(), c.wsHeaders())
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

// 探测 HTTP 推送是否可用。
func (c *Client) tryHTTPOnce(ctx context.Context) bool {
	u, err := url.Parse(c.config.MasterURL)
	if err != nil {
		return false
	}
	u.Path = constants.PathRemoteHeartbeat

	req, err := c.newRequest(ctx, http.MethodPost, u.String(), []byte("{}"))
	if err != nil {
		return false
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf("[Agent] HTTP test failed: %v", err)
		return false
	}
	defer resp.Body.Close()

	c.httpAvailable = resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusUnauthorized
	return c.httpAvailable
}

// 运行 HTTP 推送上报器。
func (c *Client) runHTTPReporter(ctx context.Context) {
	defer c.wg.Done()
	c.setCurrentMode(ModeHTTP)
	c.runHTTPReporterLoop(ctx)
}

// 执行 HTTP 上报循环。
//
// 触发 return 的两条路径:
//  1. HTTP 连错 maxErrors 次 → 让 outer auto-loop 切到 pull 或 sleep
//  2. wsProbeTicker 定期 dial ws 成功 → 返回让 outer auto-loop 切回 ws (升级)
//     这是关键改动:HTTP 一直成功时,以前永远不试 ws;现在每 wsProbeInterval 探一次,
//     ws 恢复立即升级,基于 ws 的功能(SSE 流/域名探测/即时命令)就能用了。
func (c *Client) runHTTPReporterLoop(ctx context.Context) {
	trafficTicker := time.NewTicker(c.config.TrafficReportInterval)
	defer c.registerTrafficTicker(trafficTicker)() // master push config_update 时 Reset 该 ticker
	speedTicker := time.NewTicker(c.config.SpeedReportInterval)
	heartbeatTicker := time.NewTicker(constants.WebSocketHeartbeatInterval)
	wsProbeTicker := time.NewTicker(60 * time.Second) // 每分钟探一次 ws 是否恢复
	defer trafficTicker.Stop()
	defer speedTicker.Stop()
	defer heartbeatTicker.Stop()
	defer wsProbeTicker.Stop()

	c.sendHeartbeatHTTP(ctx)
	c.sendTrafficHTTP(ctx)
	c.sendSpeedHTTP(ctx)

	consecutiveErrors := 0
	maxErrors := 5

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-trafficTicker.C:
			if err := c.sendTrafficHTTP(ctx); err != nil {
				consecutiveErrors++
				if consecutiveErrors >= maxErrors {
					log.Printf("[Agent] Too many HTTP errors, will retry connection modes")
					return
				}
			} else {
				consecutiveErrors = 0
			}
		case <-speedTicker.C:
			if err := c.sendSpeedHTTP(ctx); err != nil {
				log.Printf("[Agent] Failed to send speed via HTTP: %v", err)
			}
			c.evaluateSpeedLimits()
		case <-heartbeatTicker.C:
			if err := c.sendHeartbeatHTTP(ctx); err != nil {
				consecutiveErrors++
				if consecutiveErrors >= maxErrors {
					log.Printf("[Agent] Too many HTTP errors, will retry connection modes")
					return
				}
			} else {
				consecutiveErrors = 0
			}
		case <-wsProbeTicker.C:
			// 探测 ws 是否可用(只对支持 ws 的主控 URL 探;tryWebSocketOnce 内部判断 scheme)
			if err := c.tryWebSocketOnce(ctx); err == nil {
				log.Printf("[Agent] WebSocket recovered, upgrading from HTTP mode")
				return
			}
		}
	}
}

// 通过 HTTP POST 发送流量数据。
func (c *Client) sendTrafficHTTP(ctx context.Context) error {
	stats, err := c.collectLocalMetrics()
	if err != nil {
		stats = &collector.XrayStats{}
	}

	now := time.Now()
	c.lastUserTrafficSnap = nil
	c.lastLimitEvalTime = now

	// 系统级网卡累计,见 sendTrafficData 同段注释 —— WS / HTTP 两条上报路径保持一致。
	sysRx, sysTx := c.getSystemNetworkStats()
	payloadMap := map[string]interface{}{
		"stats": stats,
		"system": map[string]int64{
			"rx_total":       sysRx,
			"tx_total":       sysTx,
			"boot_time_unix": c.startTime.Unix(),
		},
	}

	// 伪装探针真数据:与 sendTrafficData(WS 路径)完全一致。
	// 这段之前只写在 WS 路径里,HTTP/pull 模式的服务器在伪装页上就只剩流量和网速,
	// CPU/内存/硬盘/延迟全缺 —— 主控那边的 ring 根本收不到数据。
	if c.probeAnyEnabled() {
		if sm := c.collectProbeSysMetrics(); sm != nil {
			payloadMap["sysmetrics"] = sm
		}
		if lat := c.probeLatestPingSnapshot(); len(lat) > 0 {
			payloadMap["latency"] = lat
		}
	}

	payload, _ := json.Marshal(payloadMap)

	u, err := url.Parse(c.config.MasterURL)
	if err != nil {
		return err
	}
	u.Path = constants.PathRemoteTraffic

	resp, err := c.doEncryptedHTTP(ctx, u.String(), payload)
	if err != nil {
		return err
	}

	// HTTP-mode 没有持久连接,master 把 config_update 捎带在 traffic 响应里。
	// 解析失败/无 config_updates 字段时静默跳过,不影响 traffic 上报流程。
	var respWrap struct {
		ConfigUpdates map[string]string `json:"config_updates"`
	}
	if jerr := json.Unmarshal(resp, &respWrap); jerr == nil && len(respWrap.ConfigUpdates) > 0 {
		c.handleConfigUpdate(respWrap.ConfigUpdates)
	}

	debugLogf("[Agent] Sent traffic data via HTTP: %d inbounds, %d outbounds, %d users",
		len(stats.Inbound), len(stats.Outbound), len(stats.User))
	return nil
}

// 通过 HTTP POST 发送速率数据。
func (c *Client) sendSpeedHTTP(ctx context.Context) error {
	uploadSpeed, downloadSpeed := c.collectSpeed()

	payload, _ := json.Marshal(map[string]interface{}{
		"upload_speed":   uploadSpeed,
		"download_speed": downloadSpeed,
	})

	u, err := url.Parse(c.config.MasterURL)
	if err != nil {
		return err
	}
	u.Path = constants.PathRemoteSpeed

	if _, err := c.doEncryptedHTTP(ctx, u.String(), payload); err != nil {
		return err
	}

	debugLogf("[Agent] Sent speed via HTTP: ↑%d B/s ↓%d B/s", uploadSpeed, downloadSpeed)
	return nil
}

// 通过 HTTP POST 发送心跳。
func (c *Client) sendHeartbeatHTTP(ctx context.Context) error {
	listenPort, _ := strconv.Atoi(c.config.ListenPort)
	// HTTP 模式下 master 看到的 RemoteAddr 可能是 v6(agent 用 v6 拨号 / CDN 入站 v6),
	// 若不上报 public_ipv4,master 只能用 RemoteAddr → 把 v6 错写进 db.ip_address 字段 →
	// IPv4-only master 反向请求全部 502。这里跟 WS auth/heartbeat 同款,把后台 ipProbeLoop
	// 缓存的 v4/v6 一起上报,master 端优先用,fallback 才用 RemoteAddr 并强校验类型。
	payload, _ := json.Marshal(map[string]interface{}{
		"boot_time":   c.startTime.Unix(),
		"listen_port": listenPort,
		"local_time":  time.Now().Unix(),
		"public_ipv4": c.getPublicIPv4(),
		"public_ipv6": c.getPublicIPv6(),
	})

	u, err := url.Parse(c.config.MasterURL)
	if err != nil {
		return err
	}
	u.Path = constants.PathRemoteHeartbeat

	respBody, err := c.doEncryptedHTTP(ctx, u.String(), payload)
	if err != nil {
		return err
	}

	var hbResp struct {
		ServerTime int64 `json:"server_time"`
	}
	if err := json.Unmarshal(respBody, &hbResp); err == nil && hbResp.ServerTime > 0 {
		if drift := time.Now().Unix() - hbResp.ServerTime; drift > 10 || drift < -10 {
			log.Printf("[Agent] Clock drift detected: local time is %+ds from master", drift)
		}
	}

	return nil
}

// 通过 HTTP 发送加密请求，自动处理密钥交换和会话管理。
func (c *Client) doEncryptedHTTP(ctx context.Context, urlStr string, payload []byte) ([]byte, error) {
	if c.masterPubKey == nil {
		req, err := c.newRequest(ctx, http.MethodPost, urlStr, payload)
		if err != nil {
			return nil, err
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
		}
		return body, nil
	}

	c.httpSessionMu.Lock()
	session := c.httpSession
	c.httpSessionMu.Unlock()

	if session == nil {
		return c.doHTTPKeyExchange(ctx, urlStr, payload)
	}

	encrypted, err := session.Encrypt(payload)
	if err != nil {
		return nil, fmt.Errorf("encrypt: %w", err)
	}

	req, err := c.newRequest(ctx, http.MethodPost, urlStr, encrypted)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Encrypted", "1")
	req.Header.Set(constants.HeaderContentType, "application/octet-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusPreconditionFailed {
		c.httpSessionMu.Lock()
		c.httpSession = nil
		c.httpSessionMu.Unlock()
		log.Printf("[Agent] HTTP session expired, re-negotiating")
		return c.doHTTPKeyExchange(ctx, urlStr, payload)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	if resp.Header.Get("X-Encrypted") == "1" {
		decrypted, err := session.Decrypt(body)
		if err != nil {
			return nil, fmt.Errorf("decrypt response: %w", err)
		}
		return decrypted, nil
	}
	return body, nil
}

// 执行 HTTP 密钥交换并发送请求。
func (c *Client) doHTTPKeyExchange(ctx context.Context, urlStr string, payload []byte) ([]byte, error) {
	agentPriv, agentPub, err := securechan.GenerateEphemeral()
	if err != nil {
		return nil, fmt.Errorf("generate ephemeral key: %w", err)
	}

	req, err := c.newRequest(ctx, http.MethodPost, urlStr, payload)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Key-Exchange", base64.StdEncoding.EncodeToString(agentPub))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	if kxResp := resp.Header.Get("X-Key-Exchange"); kxResp != "" {
		parts := strings.SplitN(kxResp, "|", 2)
		if len(parts) == 2 {
			masterEphPub, err1 := base64.StdEncoding.DecodeString(parts[0])
			sig, err2 := base64.StdEncoding.DecodeString(parts[1])
			if err1 == nil && err2 == nil && len(masterEphPub) == 32 {
				if securechan.Verify(c.masterPubKey, masterEphPub, sig) {
					sharedSecret, err := securechan.ComputeSharedSecret(agentPriv, masterEphPub)
					if err == nil {
						newSession, err := securechan.DeriveSession(sharedSecret, agentPub, masterEphPub, false)
						if err == nil {
							c.httpSessionMu.Lock()
							c.httpSession = newSession
							c.httpSessionMu.Unlock()
							log.Printf("[Agent] HTTP key exchange completed")
						}
					}
				} else {
					log.Printf("[Agent] HTTP key exchange: master signature verification failed")
				}
			}
		}
	}

	return body, nil
}

// 在拉取模式下持续上报流量，保持在线状态。
func (c *Client) runPullModeWithTrafficReport(ctx context.Context, duration time.Duration) {
	trafficTicker := time.NewTicker(c.config.TrafficReportInterval)
	defer c.registerTrafficTicker(trafficTicker)() // master push config_update 时 Reset 该 ticker
	defer trafficTicker.Stop()

	timeout := time.After(duration)

	if err := c.sendTrafficHTTP(ctx); err != nil {
		log.Printf("[Agent] Pull mode traffic report failed: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-timeout:
			return
		case <-c.reconnectSignal:
			// IP 漂移 / 外部触发 → 立刻退出 backoff,让 outer loop 重试 WS
			log.Printf("[Agent] Pull-mode backoff interrupted by reconnect signal")
			return
		case <-trafficTicker.C:
			if err := c.sendTrafficHTTP(ctx); err != nil {
				log.Printf("[Agent] Pull mode traffic report failed: %v", err)
			}
		}
	}
}

// 在等待期间继续上报流量 / 心跳 / 速率 — WS 模式 reconnect 退避期间的 HTTP 全降级。
//
// 设计:WS 不可用时所有上报接口都降级 HTTP,而不仅 traffic。原版只跑 traffic 导致:
//   - sendHeartbeatHTTP 不发 → master 拿不到 agent 上报的 public_ipv4/v6 → db.ip_address
//     被旧值卡住(或被 WS handleAuth 的历史 srcIP 卡住,见 173.249.198.102 现网症状)
//   - sendSpeedHTTP 不发 → master 端 current_upload_speed / current_download_speed 停滞
//
// 阈值保护:duration <= PullModeTrafficReportThreshold 时跳过入口立即跑那一发,
// 避免短 backoff 期间 spam(ticker 仍按各自周期 fire,这是冷启动后正常节奏)。
func (c *Client) waitWithTrafficReport(ctx context.Context, duration time.Duration) {
	if duration <= 0 {
		return
	}

	if duration > constants.PullModeTrafficReportThreshold {
		if err := c.sendHeartbeatHTTP(ctx); err != nil {
			log.Printf("[Agent] Heartbeat report during backoff failed: %v", err)
		}
		if err := c.sendTrafficHTTP(ctx); err != nil {
			log.Printf("[Agent] Traffic report during backoff failed: %v", err)
		}
		if err := c.sendSpeedHTTP(ctx); err != nil {
			log.Printf("[Agent] Speed report during backoff failed: %v", err)
		}
	}

	trafficTicker := time.NewTicker(c.config.TrafficReportInterval)
	defer c.registerTrafficTicker(trafficTicker)() // master push config_update 时 Reset 该 ticker
	defer trafficTicker.Stop()
	speedTicker := time.NewTicker(c.config.SpeedReportInterval)
	defer speedTicker.Stop()
	heartbeatTicker := time.NewTicker(constants.WebSocketHeartbeatInterval)
	defer heartbeatTicker.Stop()

	timeout := time.After(duration)

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-timeout:
			return
		case <-c.reconnectSignal:
			// IP 漂移 / 外部触发 → 立刻退出 backoff,让 outer loop 重试 WS
			log.Printf("[Agent] WS backoff interrupted by reconnect signal")
			return
		case <-trafficTicker.C:
			if err := c.sendTrafficHTTP(ctx); err != nil {
				log.Printf("[Agent] Traffic report during backoff failed: %v", err)
			}
		case <-speedTicker.C:
			if err := c.sendSpeedHTTP(ctx); err != nil {
				log.Printf("[Agent] Speed report during backoff failed: %v", err)
			}
		case <-heartbeatTicker.C:
			if err := c.sendHeartbeatHTTP(ctx); err != nil {
				log.Printf("[Agent] Heartbeat report during backoff failed: %v", err)
			}
		}
	}
}

// 通过 WebSocket 发送速率数据。
func (c *Client) sendSpeedData(conn *websocket.Conn) error {
	uploadSpeed, downloadSpeed := c.collectSpeed()

	payload, _ := json.Marshal(map[string]interface{}{
		"upload_speed":   uploadSpeed,
		"download_speed": downloadSpeed,
	})

	msg := map[string]interface{}{
		"type":    "speed",
		"payload": json.RawMessage(payload),
	}

	if err := c.writeEncrypted(conn, msg); err != nil {
		return err
	}

	debugLogf("[Agent] Sent speed data: ↑%d B/s ↓%d B/s", uploadSpeed, downloadSpeed)
	return nil
}

// 基于系统网卡统计计算当前上下行速率。
func (c *Client) collectSpeed() (uploadSpeed, downloadSpeed int64) {
	c.speedMu.Lock()
	defer c.speedMu.Unlock()

	rxBytes, txBytes := c.getSystemNetworkStats()

	now := time.Now()

	if !c.lastSampleTime.IsZero() && c.lastRxBytes > 0 {
		elapsed := now.Sub(c.lastSampleTime).Seconds()
		if elapsed > 0 {
			uploadSpeed = int64(float64(txBytes-c.lastTxBytes) / elapsed)
			downloadSpeed = int64(float64(rxBytes-c.lastRxBytes) / elapsed)

			if uploadSpeed < 0 {
				uploadSpeed = 0
			}
			if downloadSpeed < 0 {
				downloadSpeed = 0
			}
		}
	}

	c.lastRxBytes = rxBytes
	c.lastTxBytes = txBytes
	c.lastSampleTime = now

	return uploadSpeed, downloadSpeed
}

// 基于流量快照评估自动限速规则（每个 speed tick 调用）。
func (c *Client) evaluateSpeedLimits() {
	if c.embeddedXray == nil {
		return
	}
	monitor := c.embeddedXray.GetSpeedMonitor()
	if monitor == nil {
		return
	}

	now := time.Now()
	snapshot := c.embeddedXray.SnapshotUserTraffic()
	if snapshot == nil {
		return
	}

	if c.lastLimitEvalTime.IsZero() || c.lastUserTrafficSnap == nil {
		c.lastLimitEvalTime = now
		c.lastUserTrafficSnap = snapshot
		return
	}

	elapsed := now.Sub(c.lastLimitEvalTime)
	if elapsed <= 0 {
		return
	}

	userDeltas := make(map[string]int64, len(snapshot))
	for email, total := range snapshot {
		prev := c.lastUserTrafficSnap[email]
		delta := total - prev
		if delta > 0 {
			userDeltas[email] = delta
		}
	}

	c.lastLimitEvalTime = now
	c.lastUserTrafficSnap = snapshot

	if len(userDeltas) > 0 {
		monitor.Evaluate(userDeltas, elapsed)
	}
}

// 从 /proc/net/dev 读取网卡统计。
func (c *Client) getSystemNetworkStats() (rxBytes, txBytes int64) {
	data, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		log.Printf("[Agent] Failed to read /proc/net/dev: %v", err)
		return 0, 0
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		iface := strings.TrimSpace(parts[0])
		if !isPhysicalInterface(iface) {
			continue
		}

		fields := strings.Fields(parts[1])
		if len(fields) < 10 {
			continue
		}

		rx, err1 := strconv.ParseInt(fields[0], 10, 64)
		tx, err2 := strconv.ParseInt(fields[8], 10, 64)
		if err1 == nil && err2 == nil {
			rxBytes += rx
			txBytes += tx
		}
	}

	return rxBytes, txBytes
}

var virtualInterfacePrefixes = []string{
	"lo", "docker", "veth", "br-", "virbr", "vnet",
	"flannel", "cni", "calico", "tunl", "wg",
	"tailscale", "tun", "tap", "dummy",
}

func isPhysicalInterface(name string) bool {
	for _, prefix := range virtualInterfacePrefixes {
		if strings.HasPrefix(name, prefix) {
			return false
		}
	}
	return true
}

// AuthError 表示鉴权失败错误。
type AuthError struct {
	Message string
	Code    string // "token_expired", "token_invalid", "server_error"
}

func (e *AuthError) Error() string {
	return "authentication failed: " + e.Message
}

// 判断是否为 token 无效错误。
func (e *AuthError) IsTokenInvalid() bool {
	return e.Code == "token_invalid" || e.Message == "Invalid token"
}

// WebSocket 消息类型
const (
	WSMsgTypeCertDeploy          = "cert_deploy"
	WSMsgTypeTokenUpdate         = "token_update"
	WSMsgTypeScanResult          = "scan_result"
	WSMsgTypeDomainLatencyProbe  = "domain_latency_probe"
	WSMsgTypeDomainLatencyResult = "domain_latency_result"
	WSMsgTypeHeartbeatAck        = "heartbeat_ack"
	WSMsgTypeLimiterConfig       = "limiter_config"
	WSMsgTypeLicenseStatus       = "license_status"
	WSMsgTypeConfigUpdate        = "config_update"
	WSMsgTypeRPCCall             = "rpc_call"        // master 反向 RPC 请求(替代 /api/child/* HTTP)
	WSMsgTypeRPCReply            = "rpc_reply"       // agent 执行后返回响应(流式调用也用它作 end 帧)
	WSMsgTypeRPCStreamData       = "rpc_stream_data" // 流式调用中间数据帧 — 替代 /api/child/xxx-stream SSE
)

// WSCertDeployPayload 是主控端下发的证书部署指令。
type WSCertDeployPayload struct {
	Domain   string `json:"domain"`
	CertPEM  string `json:"cert_pem"`
	KeyPEM   string `json:"key_pem"`
	CertPath string `json:"cert_path"`
	KeyPath  string `json:"key_path"`
	Reload   string `json:"reload"`
}

// WSTokenUpdatePayload 是主控端下发的 token 更新指令。
type WSTokenUpdatePayload struct {
	ServerToken string    `json:"server_token"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// WSDomainLatencyProbePayload 是主控端下发的域名延迟探测请求。
type WSDomainLatencyProbePayload struct {
	RequestID string   `json:"request_id"`
	Domains   []string `json:"domains"`
	TimeoutMs int      `json:"timeout_ms"`
}

// WSLimiterConfigPayload 是主控端下发的限速配置。
type WSLimiterConfigPayload struct {
	InboundTag     string                        `json:"inbound_tag"`
	NodeLimit      uint64                        `json:"node_limit"`
	Users          []WSUserLimitInfo             `json:"users"`
	AutoSpeedRules []embedded.AutoSpeedLimitRule `json:"auto_speed_rules,omitempty"`
}

// WSUserLimitInfo 是单个用户的限速和连接数配置。
// DeviceLimit 现语义 = 并发连接上限(0=不限);ConnGroup 为连接数计数分组键(同组共享配额)。
type WSUserLimitInfo struct {
	UID         int    `json:"uid"`
	Email       string `json:"email"`
	SpeedLimit  uint64 `json:"speed_limit"`
	DeviceLimit int    `json:"device_limit"`
	ConnGroup   string `json:"conn_group,omitempty"` // "<user>|<物理父节点ID>";空=退化按 email 计数(老主控兼容)
}

// LicenseStatus 表示主控端下发的许可证状态。
type LicenseStatus struct {
	Valid      bool             `json:"valid"`
	MaxServers int              `json:"max_servers"`
	ExpiresAt  string           `json:"expires_at,omitempty"`
	Plan       *LicensePlanInfo `json:"plan,omitempty"`
}

// LicensePlanInfo 表示套餐信息。
type LicensePlanInfo struct {
	Name        string   `json:"name"`
	DisplayName string   `json:"display_name"`
	MaxServers  int      `json:"max_servers"`
	MaxNodes    int      `json:"max_nodes"`
	MaxUsers    int      `json:"max_users"`
	Features    []string `json:"features"`
}

func (s *LicenseStatus) HasFeature(name string) bool {
	if s == nil || s.Plan == nil {
		return false
	}
	for _, f := range s.Plan.Features {
		if f == name {
			return true
		}
	}
	return false
}

// 处理主控端下发的消息。
func (c *Client) handleMessage(conn *websocket.Conn, message []byte) {
	var msg struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}

	if err := json.Unmarshal(message, &msg); err != nil {
		log.Printf("[Agent] Failed to parse message: %v", err)
		return
	}

	switch msg.Type {
	case WSMsgTypeCertDeploy:
		var payload WSCertDeployPayload
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			log.Printf("[Agent] Failed to parse cert_deploy payload: %v", err)
			return
		}
		go c.handleCertDeploy(payload)
	case WSMsgTypeTokenUpdate:
		var payload WSTokenUpdatePayload
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			log.Printf("[Agent] Failed to parse token_update payload: %v", err)
			return
		}
		c.handleTokenUpdate(payload)
	case WSMsgTypeDomainLatencyProbe:
		var payload WSDomainLatencyProbePayload
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			log.Printf("[Agent] Failed to parse domain_latency_probe payload: %v", err)
			return
		}
		go c.handleDomainLatencyProbe(conn, payload)
	case WSMsgTypeHeartbeatAck:
		var payload struct {
			ServerTime int64 `json:"server_time"`
		}
		if err := json.Unmarshal(msg.Payload, &payload); err == nil && payload.ServerTime > 0 {
			if drift := time.Now().Unix() - payload.ServerTime; drift > 10 || drift < -10 {
				log.Printf("[Agent] Clock drift detected: local time is %+ds from master", drift)
			}
		}
	case WSMsgTypeLimiterConfig:
		var payload WSLimiterConfigPayload
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			log.Printf("[Agent] Failed to parse limiter_config payload: %v", err)
			return
		}
		c.handleLimiterConfig(payload)
	case WSMsgTypeLicenseStatus:
		var payload LicenseStatus
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			log.Printf("[Agent] Failed to parse license_status payload: %v", err)
			return
		}
		c.handleLicenseStatus(payload)
	case WSMsgTypeConfigUpdate:
		var payload map[string]string
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			log.Printf("[Agent] Failed to parse config_update payload: %v", err)
			return
		}
		c.handleConfigUpdate(payload)
	case WSMsgTypeRPCCall:
		// master 反向 RPC:把 payload 转成 *http.Request 喂给共享的 rpcMux,响应回 master。
		// 必须 go 起新协程 — handler 内部可能阻塞数秒(xray restart / 路由切换等),不能堵主循环。
		var payload WSRPCCallPayload
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			log.Printf("[Agent] Failed to parse rpc_call payload: %v", err)
			return
		}
		go c.handleRPCCall(conn, payload)
	default:
		// 忽略未知消息类型
	}
}

// 处理主控端下发的证书部署。
func (c *Client) handleCertDeploy(payload WSCertDeployPayload) {
	log.Printf("[Agent] Received cert_deploy for domain: %s, target: %s", payload.Domain, payload.Reload)

	if err := deployCert(payload.CertPEM, payload.KeyPEM, payload.CertPath, payload.KeyPath, payload.Reload); err != nil {
		log.Printf("[Agent] cert_deploy failed for %s: %v", payload.Domain, err)
	} else {
		log.Printf("[Agent] cert_deploy succeeded for %s", payload.Domain)
	}
}

func deployCert(certPEM, keyPEM, certPath, keyPath, reloadTarget string) error {
	if certPath == "" || keyPath == "" {
		return fmt.Errorf("deploy paths are required")
	}
	// 与 HTTP/RPC 侧 deployCertFiles 同一套防御:内容必须为真实 PEM 证书/私钥,
	// 路径不得落在敏感目录。防止主控被攻破后经 WS cert_deploy 任意写文件 → RCE。
	if err := util.ValidateCertKeyPEM(certPEM, keyPEM); err != nil {
		return err
	}
	if err := util.CertPathSafe(certPath); err != nil {
		return fmt.Errorf("cert path: %w", err)
	}
	if err := util.CertPathSafe(keyPath); err != nil {
		return fmt.Errorf("key path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(certPath), 0755); err != nil {
		return fmt.Errorf("create cert dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(keyPath), 0755); err != nil {
		return fmt.Errorf("create key dir: %w", err)
	}
	if err := os.WriteFile(certPath, []byte(certPEM), 0644); err != nil {
		return fmt.Errorf("write cert: %w", err)
	}
	if err := os.WriteFile(keyPath, []byte(keyPEM), 0600); err != nil {
		return fmt.Errorf("write key: %w", err)
	}

	switch reloadTarget {
	case "nginx":
		return reloadNginxCmd()
	case "xray":
		return xrayctl.RestartXray("auto", "")
	case "both":
		if err := reloadNginxCmd(); err != nil {
			return err
		}
		return xrayctl.RestartXray("auto", "")
	}
	return nil
}

func reloadNginxCmd() error {
	for _, bin := range constants.NginxBinarySearchPaths {
		if path, err := exec.LookPath(bin); err == nil {
			return runCmd(path, "-s", "reload")
		}
	}
	return runCmd("systemctl", "reload", "nginx")
}

func runCmd(name string, args ...string) error {
	if output, err := exec.Command(name, args...).CombinedOutput(); err != nil {
		return fmt.Errorf("%s: %s: %w", name, string(output), err)
	}
	return nil
}

// 处理主控端下发的 token 更新。
func (c *Client) handleTokenUpdate(payload WSTokenUpdatePayload) {
	log.Printf("[Agent] Received token update from master, new token expires at %s", payload.ExpiresAt.Format(time.RFC3339))

	// 更新内存中的 token
	c.config.Token = payload.ServerToken

	log.Printf("[Agent] Token updated successfully in memory")
}

// 处理主控端下发的限速配置。
func (c *Client) handleLicenseStatus(payload LicenseStatus) {
	c.licenseMu.Lock()
	c.licenseStatus = &payload
	c.licenseMu.Unlock()

	planName := "FREE"
	if payload.Plan != nil {
		planName = payload.Plan.DisplayName
	}
	log.Printf("[Agent] License status updated: valid=%v plan=%s max_servers=%d", payload.Valid, planName, payload.MaxServers)
}

func (c *Client) HasProFeature(name string) bool {
	c.licenseMu.RLock()
	defer c.licenseMu.RUnlock()
	return c.licenseStatus.HasFeature(name)
}

func (c *Client) handleLimiterConfig(payload WSLimiterConfigPayload) {
	if c.embeddedXray == nil {
		log.Printf("[Agent] Ignoring limiter_config: not in embedded mode")
		return
	}
	if !c.HasProFeature("limiter") {
		log.Printf("[Agent] Ignoring limiter_config: limiter feature not licensed")
		return
	}

	users := make([]limiter.UserInfo, len(payload.Users))
	for i, u := range payload.Users {
		users[i] = limiter.UserInfo{
			UID:         u.UID,
			Email:       u.Email,
			SpeedLimit:  u.SpeedLimit,
			DeviceLimit: u.DeviceLimit,
			ConnGroup:   u.ConnGroup,
		}
	}

	l := c.embeddedXray.GetLimiter()
	if l == nil {
		return
	}

	// 用 Sync 而非 Add:Add 会重建 BucketHub,存量连接持有的旧桶不会被更新,
	// 改限速对已连上的用户不生效(要等他重连)。见 SyncInboundLimiter 的说明。
	l.SyncInboundLimiter(payload.InboundTag, payload.NodeLimit, users)

	if monitor := c.embeddedXray.GetSpeedMonitor(); monitor != nil {
		monitor.SetLimiter(l)
		if len(payload.AutoSpeedRules) > 0 {
			monitor.UpdateRules(payload.AutoSpeedRules)
			log.Printf("[Agent] Updated auto speed rules: %d rules", len(payload.AutoSpeedRules))
		}
	}

	// 日志包含每个用户的限速值,便于线上排查"为什么限速不生效"——
	// 常见原因 1: master 端 SpeedLimitMbps 是 0; 常见原因 2: vision/splice 协议会绕开 RateWriter。
	var userSpeeds []string
	for _, u := range users {
		userSpeeds = append(userSpeeds, fmt.Sprintf("%s=%dB/s", u.Email, u.SpeedLimit))
	}
	log.Printf("[Agent] Updated limiter for inbound %s: %d users, node_limit=%d, per-user=[%s]",
		payload.InboundTag, len(users), payload.NodeLimit, strings.Join(userSpeeds, ","))
}

// 采集所有 inbound 的在线设备信息。
func (c *Client) collectOnlineUsers() map[string][]string {
	if c.embeddedXray == nil {
		return nil
	}
	result := make(map[string][]string)
	tags := c.embeddedXray.ListInbounds()
	for _, tag := range tags {
		users := c.embeddedXray.GetOnlineUsers(tag)
		for email, ips := range users {
			result[email] = ips
		}
	}
	return result
}

// 在本机探测域名延迟并回传结果。
func (c *Client) handleDomainLatencyProbe(conn *websocket.Conn, payload WSDomainLatencyProbePayload) {
	log.Printf("[Agent] Received domain_latency_probe: %d domains, timeout=%dms", len(payload.Domains), payload.TimeoutMs)

	timeoutMs := payload.TimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = constants.DomainProbeDefaultTimeoutMS
	}
	if timeoutMs < constants.DomainProbeMinTimeoutMS {
		timeoutMs = constants.DomainProbeMinTimeoutMS
	}
	if timeoutMs > constants.DomainProbeMaxTimeoutMS {
		timeoutMs = constants.DomainProbeMaxTimeoutMS
	}
	timeout := time.Duration(timeoutMs) * time.Millisecond

	type probeResult struct {
		Domain       string `json:"domain"`
		Target       string `json:"target"`
		Success      bool   `json:"success"`
		LatencyMs    int64  `json:"latency_ms,omitempty"`
		Error        string `json:"error,omitempty"`
		NginxSSLPort int    `json:"nginx_ssl_port,omitempty"`
	}

	// 读取本机 nginx 配置，构造 domain -> ssl 端口映射
	nginxPortMap := readNginxSSLPorts(payload.Domains)

	results := make([]probeResult, 0, len(payload.Domains))
	resultCh := make(chan probeResult, len(payload.Domains))
	sem := make(chan struct{}, constants.DomainProbeConcurrency)
	var wg sync.WaitGroup

	for _, domain := range payload.Domains {
		wg.Add(1)
		domain := domain
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			host := domain
			port := "443"
			if h, p, err := net.SplitHostPort(domain); err == nil && h != "" && p != "" {
				host = h
				port = p
			}
			if host == "" {
				resultCh <- probeResult{Domain: domain, Target: domain, Success: false, Error: "empty host"}
				return
			}
			target := net.JoinHostPort(host, port)
			start := time.Now()
			tcpConn, err := net.DialTimeout("tcp", target, timeout)
			if err != nil {
				resultCh <- probeResult{Domain: host, Target: target, Success: false, Error: err.Error()}
				return
			}
			_ = tcpConn.Close()
			resultCh <- probeResult{Domain: host, Target: target, Success: true, LatencyMs: time.Since(start).Milliseconds(), NginxSSLPort: nginxPortMap[host]}
		}()
	}

	wg.Wait()
	close(resultCh)
	for r := range resultCh {
		results = append(results, r)
	}

	// 排序：成功优先，再按延迟升序
	sort.Slice(results, func(i, j int) bool {
		if results[i].Success != results[j].Success {
			return results[i].Success
		}
		if !results[i].Success {
			return results[i].Domain < results[j].Domain
		}
		if results[i].LatencyMs == results[j].LatencyMs {
			return results[i].Domain < results[j].Domain
		}
		return results[i].LatencyMs < results[j].LatencyMs
	})

	response := map[string]any{
		"request_id": payload.RequestID,
		"success":    true,
		"results":    results,
	}
	respBytes, err := json.Marshal(response)
	if err != nil {
		log.Printf("[Agent] Failed to marshal domain_latency_result: %v", err)
		return
	}

	msg := map[string]any{
		"type":    WSMsgTypeDomainLatencyResult,
		"payload": json.RawMessage(respBytes),
	}

	if err := c.writeEncrypted(conn, msg); err != nil {
		log.Printf("[Agent] Failed to send domain_latency_result: %v", err)
		return
	}

	log.Printf("[Agent] Sent domain_latency_result: %d results", len(results))
}

// readNginxSSLPorts 读取 nginx 配置并返回 domain -> SSL 端口映射。
// 会在常见 nginx 配置目录下查找 servers/{domain}.conf。
func readNginxSSLPorts(domains []string) map[string]int {
	result := make(map[string]int)
	if len(domains) == 0 {
		return result
	}

	confDirs := constants.NginxSSLServerDirPaths

	for _, domain := range domains {
		host := domain
		if h, _, err := net.SplitHostPort(domain); err == nil && h != "" {
			host = h
		}
		for _, dir := range confDirs {
			confPath := filepath.Join(dir, host+".conf")
			data, err := os.ReadFile(confPath)
			if err != nil {
				continue
			}
			if port := extractSSLListenPort(string(data)); port > 0 {
				result[host] = port
				break
			}
		}
	}
	return result
}

// 提取 nginx 配置块中第一个 "listen <port> ssl" 端口。
func extractSSLListenPort(conf string) int {
	// 匹配示例: listen 58443 ssl
	for _, line := range strings.Split(conf, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "listen") {
			continue
		}
		// 去掉 "listen" 前缀和结尾分号
		rest := strings.TrimPrefix(line, "listen")
		rest = strings.TrimRight(rest, ";")
		rest = strings.TrimSpace(rest)
		fields := strings.Fields(rest)
		if len(fields) < 2 {
			continue
		}
		// 判断字段中是否包含 "ssl"
		hasSSL := false
		for _, f := range fields[1:] {
			if f == "ssl" {
				hasSSL = true
				break
			}
		}
		if !hasSSL {
			continue
		}
		// 第一个字段是端口（或 [::]:port）
		portStr := fields[0]
		// 兼容 [::]:port 形式
		if idx := strings.LastIndex(portStr, ":"); idx >= 0 {
			portStr = portStr[idx+1:]
		}
		port, err := strconv.Atoi(portStr)
		if err == nil && port > 0 {
			return port
		}
	}
	return 0
}

// 扫描本机 xray 状态并上报主控端。
func (c *Client) sendScanResult(conn *websocket.Conn) {
	xrayRunning := false
	xrayVersion := ""

	if c.config.XrayMode == "embedded" && c.embeddedXray != nil {
		xrayRunning = c.embeddedXray.IsRunning()
	} else {
		cmd := exec.Command("xray", "version")
		if out, err := cmd.Output(); err == nil {
			xrayVersion = strings.TrimSpace(strings.Split(string(out), "\n")[0])
		}
		if exec.Command("systemctl", "is-active", "--quiet", "xray").Run() == nil {
			xrayRunning = true
		}
	}

	// 从配置读取入站列表（使用 3-tier 发现）
	var inbounds []map[string]interface{}
	paths := discovery.Discover()
	if paths.ConfigPath != "" || paths.ConfDir != "" {
		if cfg, err := xrayconf.ReadConfig(paths.ConfigPath, paths.ConfDir); err == nil {
			if ibs, ok := cfg["inbounds"].([]interface{}); ok {
				for _, ib := range ibs {
					if m, ok := ib.(map[string]interface{}); ok {
						if tag, _ := m["tag"].(string); tag == "api" {
							continue
						}
						inbounds = append(inbounds, m)
					}
				}
			}
		}
	}

	// Phase 3B: 上报 limiter KickCounter(累计每个 email 被「踢最旧」的总次数,自 agent 启动起单调递增)
	// 主控按 delta 算单周期增量,delta>0 触发 tg 通知。
	// 老主控收到此字段直接忽略,无影响。
	deviceKicks := limiter.SnapshotKickCounter()

	payload, _ := json.Marshal(map[string]interface{}{
		"xray_running": xrayRunning,
		"xray_version": xrayVersion,
		"inbounds":     inbounds,
		"device_kicks": deviceKicks,
	})

	msg := map[string]interface{}{
		"type":    WSMsgTypeScanResult,
		"payload": json.RawMessage(payload),
	}

	if err := c.writeEncrypted(conn, msg); err != nil {
		log.Printf("[Agent] Failed to send scan_result: %v", err)
		return
	}
	log.Printf("[Agent] Sent scan_result: xray_running=%v, inbounds=%d", xrayRunning, len(inbounds))
}

func (c *Client) handleConfigUpdate(updates map[string]string) {
	if stealMode, ok := updates["steal_mode"]; ok && stealMode != c.config.StealMode {
		c.config.StealMode = stealMode
		if err := c.persistConfigField("steal_mode", stealMode); err != nil {
			log.Printf("[Agent] Failed to persist steal_mode: %v", err)
		} else {
			log.Printf("[Agent] Updated steal_mode to %q", stealMode)
		}
	}
	// traffic_report_interval_ms: master 修改后 push,无需重启 agent 立即应用到所有 4 处 ticker。
	// clamp 到 [1s, 60s] 避免极端值导致 master 压力暴涨或数据滞后。
	if raw, ok := updates["traffic_report_interval_ms"]; ok {
		if ms, err := strconv.Atoi(raw); err == nil {
			if ms < 1000 {
				ms = 1000
			}
			if ms > 60000 {
				ms = 60000
			}
			d := time.Duration(ms) * time.Millisecond
			if d != c.config.TrafficReportInterval {
				c.config.TrafficReportInterval = d
				resetCount := 0
				c.trafficTickers.Range(func(k, _ any) bool {
					if t, ok := k.(*time.Ticker); ok {
						t.Reset(d)
						resetCount++
					}
					return true
				})
				log.Printf("[Agent] traffic_report_interval updated to %v (reset %d active tickers)", d, resetCount)
			}
		}
	}

	// agent_log_enabled: 主控下发的 debug 日志总开关,控制流量上报等高频日志(默认关闭)。
	// 不落盘(与探针配置同),重连时主控会重新下发当前值。
	if raw, ok := updates["agent_log_enabled"]; ok {
		setDebugLogEnabled(raw == "1" || strings.EqualFold(raw, "true"))
	}

	// xray_authorized: 主控按许可证「服务器配额」下发的运行授权。超额(0)→ 停 xray;拿到名额(1)→ 启 xray。
	// 落盘以便重启时立即据此决定是否启动 xray。幂等:与当前值比对,变了才动作(避免 5min 定期兜底反复启停)。
	// 当前值 nil(首次/未配置)视为已授权 true。
	if raw, ok := updates["xray_authorized"]; ok {
		authorized := raw == "1" || strings.EqualFold(raw, "true")
		cur := c.config.XrayAuthorized.Bool(true)
		if authorized != cur {
			flex := config.FlexBool(authorized)
			c.config.XrayAuthorized = &flex
			// 必须写 true/false:字段类型是布尔,写 "1" 的话下次启动 yaml 解析直接失败
			// (cannot unmarshal !!int `1` into bool)→ systemd 无限重启。
			if err := c.persistConfigField("xray_authorized", strconv.FormatBool(authorized)); err != nil {
				log.Printf("[Agent] Failed to persist xray_authorized: %v", err)
			} else {
				log.Printf("[Agent] xray_authorized changed to %v (license quota)", authorized)
			}
			if c.onXrayAuthChange != nil {
				c.onXrayAuthChange(authorized)
			}
		}
	}

	c.handleProbeConfigUpdate(updates)
}

// handleProbeConfigUpdate 解析伪装探针采集配置(4 子开关 + ping 目标 + 间隔)。
// 这些**不落盘**(规避 config.yaml 的 JSON 注入面),重连时主控会重新下发。
func (c *Client) handleProbeConfigUpdate(updates map[string]string) {
	_, hasCPU := updates["probe_collect_cpu"]
	_, hasMem := updates["probe_collect_mem"]
	_, hasDisk := updates["probe_collect_disk"]
	_, hasPing := updates["probe_collect_ping"]
	_, hasTargets := updates["probe_ping_targets"]
	_, hasInterval := updates["probe_ping_interval_ms"]
	if !hasCPU && !hasMem && !hasDisk && !hasPing && !hasTargets && !hasInterval {
		return // 本次下发不含探针配置
	}

	c.probeMu.Lock()
	defer c.probeMu.Unlock()
	if hasCPU {
		c.probeCollectCPU = updates["probe_collect_cpu"] == "1"
	}
	if hasMem {
		c.probeCollectMem = updates["probe_collect_mem"] == "1"
	}
	if hasDisk {
		c.probeCollectDisk = updates["probe_collect_disk"] == "1"
	}
	if hasPing {
		c.probeCollectPing = updates["probe_collect_ping"] == "1"
	}
	if hasTargets {
		var targets []ProbePingTarget
		if json.Unmarshal([]byte(updates["probe_ping_targets"]), &targets) == nil {
			c.probePingTargets = targets
		}
	}
	if hasInterval {
		if ms, err := strconv.Atoi(updates["probe_ping_interval_ms"]); err == nil {
			if ms < 2000 {
				ms = 2000
			}
			// 上限与主控 SetProbeDisguise 的校验保持一致(300s),否则管理员设的长间隔会被悄悄砍短。
			if ms > 300000 {
				ms = 300000
			}
			c.probePingIntMs = ms
		}
	}
	log.Printf("[Agent] probe config updated: cpu=%v mem=%v disk=%v ping=%v targets=%d",
		c.probeCollectCPU, c.probeCollectMem, c.probeCollectDisk, c.probeCollectPing, len(c.probePingTargets))
}

// probeAnyEnabled 是否有任一采集项开启。
func (c *Client) probeAnyEnabled() bool {
	c.probeMu.Lock()
	defer c.probeMu.Unlock()
	return c.probeCollectCPU || c.probeCollectMem || c.probeCollectDisk || c.probeCollectPing
}

// collectProbeSysMetrics 按开关采一次系统指标(供 sendTrafficData 调)。无开启项返回 nil。
func (c *Client) collectProbeSysMetrics() *ProbeSysMetrics {
	c.probeMu.Lock()
	cpu, mem, disk := c.probeCollectCPU, c.probeCollectMem, c.probeCollectDisk
	if c.probeSys == nil {
		c.probeSys = newSysMetricsCollector()
	}
	sc := c.probeSys
	c.probeMu.Unlock()
	if !cpu && !mem && !disk {
		return nil
	}
	m := sc.sample(cpu, mem, disk)
	return &m
}

// probeLatestPingSnapshot 读最近一轮 ping 结果(供 traffic tick 带走)。
//
// 同一轮结果只上报一次:traffic tick 是 5s 一次,ping 间隔默认更长,不去重的话
// 同一份结果会被重复 ingest 十几次 —— 主控那边的 ring 会被重复点填满(有效窗口缩水),
// 丢包率的分母也跟着虚增。
func (c *Client) probeLatestPingSnapshot() []ProbeLatencySample {
	c.probeMu.Lock()
	defer c.probeMu.Unlock()
	if !c.probeCollectPing || len(c.probeLatestPing) == 0 {
		return nil
	}
	if c.probeLatestPingSentAt == c.probeLatestPingAt {
		return nil // 这一轮已经报过了
	}
	c.probeLatestPingSentAt = c.probeLatestPingAt
	out := make([]ProbeLatencySample, len(c.probeLatestPing))
	copy(out, c.probeLatestPing)
	return out
}

// runProbePingLoop 独立 goroutine:按 probePingIntMs 周期性 ping 目标,结果写 probeLatestPing。
// 与 traffic tick 解耦,避免每次 traffic tick 同步阻塞在 N 个 TCP 拨测上。
func (c *Client) runProbePingLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.probeMu.Lock()
			on := c.probeCollectPing
			targets := c.probePingTargets
			interval := c.probePingIntMs
			c.probeMu.Unlock()
			if interval >= 2000 {
				ticker.Reset(time.Duration(interval) * time.Millisecond)
			}
			if !on || len(targets) == 0 {
				continue
			}
			samples := probeRegions(targets)
			c.probeMu.Lock()
			c.probeLatestPing = samples
			if len(samples) > 0 {
				c.probeLatestPingAt = samples[0].At // 整轮同一个 At
			}
			c.probeMu.Unlock()
		}
	}
}

// registerTrafficTicker 把活跃 ticker 加入 registry,返回 deregister 闭包(配合 defer)。
// 用于 master push config_update 时遍历 Reset。
func (c *Client) registerTrafficTicker(t *time.Ticker) func() {
	c.trafficTickers.Store(t, struct{}{})
	return func() { c.trafficTickers.Delete(t) }
}

func (c *Client) persistConfigField(key, value string) error {
	// 净化:值不得含换行/控制字符,否则会注入任意 YAML 行到 config.yaml(主控被攻破时的持久化提权)。
	if !util.NoControlChars(value) {
		return fmt.Errorf("persistConfigField: 字段 %q 的值含非法控制字符,拒绝写入", key)
	}
	cfgPath := "/etc/mmw-agent/config.yaml"
	if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
		return nil
	}

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		return err
	}

	lines := strings.Split(string(data), "\n")
	found := false
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, key+":") {
			lines[i] = key + ": " + value
			found = true
			break
		}
	}
	if !found {
		lines = append(lines, key+": "+value)
	}

	return os.WriteFile(cfgPath, []byte(strings.Join(lines, "\n")), 0644)
}
