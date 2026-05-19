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
	lastLimitEvalTime    time.Time
	lastUserTrafficSnap  map[string]int64

	// 嵌入模式
	embeddedXray *embedded.EmbeddedXray

	// 许可证状态
	licenseStatus *LicenseStatus
	licenseMu     sync.RWMutex

	// 加密通信
	masterPubKey  ed25519.PublicKey
	wsSession     *securechan.Session
	wsSessionMu   sync.Mutex
	httpSession   *securechan.Session
	httpSessionMu sync.Mutex

	// 公网 IPv4 缓存
	publicIPv4 string
}

// 创建 agent 客户端。
func NewClient(cfg *config.Config) *Client {
	c := &Client{
		config:      cfg,
		collector:   collector.NewCollector(),
		xrayServers: cfg.XrayServers,
		stopCh:      make(chan struct{}),
		startTime:   time.Now(),
		httpClient: &http.Client{
			Timeout: constants.DefaultHTTPClientTimeout,
		},
		currentMode: ModePull,
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

// 停止客户端。
func (c *Client) Stop() {
	close(c.stopCh)
	c.wg.Wait()

	c.wsMu.Lock()
	if c.wsConn != nil {
		c.wsConn.Close()
	}
	c.wsMu.Unlock()

	log.Printf("[Agent] Stopped")
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

	dialer := websocket.Dialer{
		HandshakeTimeout: constants.WebSocketHandshakeTimeout,
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
	authPayload, _ := json.Marshal(map[string]string{
		"token": c.config.Token,
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
		err = conn.WriteMessage(websocket.BinaryMessage, envelope)
		c.wsMu.Unlock()
		return err
	}

	c.wsMu.Lock()
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

	if c.embeddedXray != nil {
		onlineUsers := c.collectOnlineUsers()
		if len(onlineUsers) > 0 {
			payloadMap["online_users"] = onlineUsers
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

	payload, _ := json.Marshal(payloadMap)

	msg := map[string]interface{}{
		"type":    "traffic",
		"payload": json.RawMessage(payload),
	}

	if err := c.writeEncrypted(conn, msg); err != nil {
		return err
	}

	log.Printf("[Agent] Sent traffic data: %d inbounds, %d outbounds, %d users",
		len(stats.Inbound), len(stats.Outbound), len(stats.User))

	return nil
}

// 发送心跳消息。
func (c *Client) sendHeartbeat(conn *websocket.Conn) error {
	if c.publicIPv4 == "" {
		c.publicIPv4 = c.detectPublicIPv4()
	}
	listenPort, _ := strconv.Atoi(c.config.ListenPort)
	payload, _ := json.Marshal(map[string]interface{}{
		"boot_time":   c.startTime,
		"listen_port": listenPort,
		"local_time":  time.Now().Unix(),
		"public_ipv4": c.publicIPv4,
	})

	msg := map[string]interface{}{
		"type":    "heartbeat",
		"payload": json.RawMessage(payload),
	}

	return c.writeEncrypted(conn, msg)
}

func (c *Client) detectPublicIPv4() string {
	urls := []string{"https://4.ipw.cn", "https://api4.ipify.org"}
	client := &http.Client{Timeout: 5 * time.Second}
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
		if ip != "" && !strings.Contains(ip, ":") {
			return ip
		}
	}
	// fallback: try to get any public IP (may be IPv6)
	fallbackURLs := []string{"https://ipw.cn", "https://api64.ipify.org"}
	for _, u := range fallbackURLs {
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
		if ip != "" {
			return ip
		}
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

	dialer := websocket.Dialer{
		HandshakeTimeout: constants.WebSocketHandshakeTimeout,
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
func (c *Client) runHTTPReporterLoop(ctx context.Context) {
	trafficTicker := time.NewTicker(c.config.TrafficReportInterval)
	speedTicker := time.NewTicker(c.config.SpeedReportInterval)
	heartbeatTicker := time.NewTicker(constants.WebSocketHeartbeatInterval)
	defer trafficTicker.Stop()
	defer speedTicker.Stop()
	defer heartbeatTicker.Stop()

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

	payload, _ := json.Marshal(map[string]interface{}{
		"stats": stats,
	})

	u, err := url.Parse(c.config.MasterURL)
	if err != nil {
		return err
	}
	u.Path = constants.PathRemoteTraffic

	if _, err := c.doEncryptedHTTP(ctx, u.String(), payload); err != nil {
		return err
	}

	log.Printf("[Agent] Sent traffic data via HTTP: %d inbounds, %d outbounds, %d users",
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

	log.Printf("[Agent] Sent speed via HTTP: ↑%d B/s ↓%d B/s", uploadSpeed, downloadSpeed)
	return nil
}

// 通过 HTTP POST 发送心跳。
func (c *Client) sendHeartbeatHTTP(ctx context.Context) error {
	listenPort, _ := strconv.Atoi(c.config.ListenPort)
	payload, _ := json.Marshal(map[string]interface{}{
		"boot_time":   c.startTime.Unix(),
		"listen_port": listenPort,
		"local_time":  time.Now().Unix(),
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
		case <-trafficTicker.C:
			if err := c.sendTrafficHTTP(ctx); err != nil {
				log.Printf("[Agent] Pull mode traffic report failed: %v", err)
			}
		}
	}
}

// 在等待期间继续上报流量。
func (c *Client) waitWithTrafficReport(ctx context.Context, duration time.Duration) {
	if duration <= 0 {
		return
	}

	if duration > constants.PullModeTrafficReportThreshold {
		if err := c.sendTrafficHTTP(ctx); err != nil {
			log.Printf("[Agent] Traffic report during backoff failed: %v", err)
		}
	}

	trafficTicker := time.NewTicker(c.config.TrafficReportInterval)
	defer trafficTicker.Stop()

	timeout := time.After(duration)

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-timeout:
			return
		case <-trafficTicker.C:
			if err := c.sendTrafficHTTP(ctx); err != nil {
				log.Printf("[Agent] Traffic report during backoff failed: %v", err)
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

	log.Printf("[Agent] Sent speed data: ↑%d B/s ↓%d B/s", uploadSpeed, downloadSpeed)
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
	InboundTag     string                       `json:"inbound_tag"`
	NodeLimit      uint64                       `json:"node_limit"`
	Users          []WSUserLimitInfo            `json:"users"`
	AutoSpeedRules []embedded.AutoSpeedLimitRule `json:"auto_speed_rules,omitempty"`
}

// WSUserLimitInfo 是单个用户的限速和设备数配置。
type WSUserLimitInfo struct {
	UID         int    `json:"uid"`
	Email       string `json:"email"`
	SpeedLimit  uint64 `json:"speed_limit"`
	DeviceLimit int    `json:"device_limit"`
}

// LicenseStatus 表示主控端下发的许可证状态。
type LicenseStatus struct {
	Valid      bool              `json:"valid"`
	MaxServers int               `json:"max_servers"`
	ExpiresAt  string            `json:"expires_at,omitempty"`
	Plan       *LicensePlanInfo  `json:"plan,omitempty"`
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
		}
	}

	l := c.embeddedXray.GetLimiter()
	if l == nil {
		return
	}

	l.AddInboundLimiter(payload.InboundTag, payload.NodeLimit, users)

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

	payload, _ := json.Marshal(map[string]interface{}{
		"xray_running": xrayRunning,
		"xray_version": xrayVersion,
		"inbounds":     inbounds,
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
}

func (c *Client) persistConfigField(key, value string) error {
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
