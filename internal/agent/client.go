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

	"mmw-agent/internal/collector"
	"mmw-agent/internal/config"
	"mmw-agent/internal/constants"

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
}

// 创建 agent 客户端。
func NewClient(cfg *config.Config) *Client {
	return &Client{
		config:      cfg,
		collector:   collector.NewCollector(),
		xrayServers: cfg.XrayServers,
		stopCh:      make(chan struct{}),
		httpClient: &http.Client{
			Timeout: constants.DefaultHTTPClientTimeout,
		},
		currentMode: ModePull, // 默认使用拉取模式
	}
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
		conn.Close()
	}()

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

	if err := conn.WriteJSON(msg); err != nil {
		return err
	}

	conn.SetReadDeadline(time.Now().Add(constants.WebSocketReadDeadline))
	_, message, err := conn.ReadMessage()
	if err != nil {
		return err
	}

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
			// 投递到消息处理通道
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
		case <-heartbeatTicker.C:
			if err := c.sendHeartbeat(conn); err != nil {
				return err
			}
		}
	}
}

// 采集并发送流量数据。
func (c *Client) sendTrafficData(conn *websocket.Conn) error {
	stats, err := c.collectLocalMetrics()
	if err != nil {
		log.Printf("[Agent] Failed to collect metrics: %v", err)
		stats = &collector.XrayStats{}
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"stats": stats,
	})

	msg := map[string]interface{}{
		"type":    "traffic",
		"payload": json.RawMessage(payload),
	}

	c.wsMu.Lock()
	err = conn.WriteJSON(msg)
	c.wsMu.Unlock()

	if err != nil {
		return err
	}

	log.Printf("[Agent] Sent traffic data: %d inbounds, %d outbounds, %d users",
		len(stats.Inbound), len(stats.Outbound), len(stats.User))

	return nil
}

// 发送心跳消息。
func (c *Client) sendHeartbeat(conn *websocket.Conn) error {
	now := time.Now()
	listenPort, _ := strconv.Atoi(c.config.ListenPort)
	payload, _ := json.Marshal(map[string]interface{}{
		"boot_time":   now,
		"listen_port": listenPort,
	})

	msg := map[string]interface{}{
		"type":    "heartbeat",
		"payload": json.RawMessage(payload),
	}

	c.wsMu.Lock()
	err := conn.WriteJSON(msg)
	c.wsMu.Unlock()

	return err
}

// 采集本机 Xray 流量指标。
func (c *Client) collectLocalMetrics() (*collector.XrayStats, error) {
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

	payload, _ := json.Marshal(map[string]interface{}{
		"stats": stats,
	})

	u, err := url.Parse(c.config.MasterURL)
	if err != nil {
		return err
	}
	u.Path = constants.PathRemoteTraffic

	req, err := c.newRequest(ctx, http.MethodPost, u.String(), payload)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
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

	req, err := c.newRequest(ctx, http.MethodPost, u.String(), payload)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	log.Printf("[Agent] Sent speed via HTTP: ↑%d B/s ↓%d B/s", uploadSpeed, downloadSpeed)
	return nil
}

// 通过 HTTP POST 发送心跳。
func (c *Client) sendHeartbeatHTTP(ctx context.Context) error {
	now := time.Now()
	listenPort, _ := strconv.Atoi(c.config.ListenPort)
	payload, _ := json.Marshal(map[string]interface{}{
		"boot_time":   now,
		"listen_port": listenPort,
	})

	u, err := url.Parse(c.config.MasterURL)
	if err != nil {
		return err
	}
	u.Path = constants.PathRemoteHeartbeat

	req, err := c.newRequest(ctx, http.MethodPost, u.String(), payload)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return nil
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

	c.wsMu.Lock()
	err := conn.WriteJSON(msg)
	c.wsMu.Unlock()

	if err != nil {
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
		return runCmd("systemctl", "restart", "xray")
	case "both":
		if err := reloadNginxCmd(); err != nil {
			return err
		}
		return runCmd("systemctl", "restart", "xray")
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
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		log.Printf("[Agent] Failed to marshal WS message: %v", err)
		return
	}

	c.wsMu.Lock()
	err = conn.WriteMessage(websocket.TextMessage, msgBytes)
	c.wsMu.Unlock()
	if err != nil {
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
	// 检查 xray 运行状态
	xrayRunning := false
	xrayVersion := ""
	cmd := exec.Command("xray", "version")
	if out, err := cmd.Output(); err == nil {
		xrayVersion = strings.TrimSpace(strings.Split(string(out), "\n")[0])
	}
	if exec.Command("systemctl", "is-active", "--quiet", "xray").Run() == nil {
		xrayRunning = true
	}

	// 从配置读取入站列表
	var inbounds []map[string]interface{}
	configPaths := constants.DefaultXrayConfigPaths
	for _, cfgPath := range configPaths {
		data, err := os.ReadFile(cfgPath)
		if err != nil {
			continue
		}
		var config map[string]interface{}
		if json.Unmarshal(data, &config) != nil {
			continue
		}
		if ibs, ok := config["inbounds"].([]interface{}); ok {
			for _, ib := range ibs {
				if m, ok := ib.(map[string]interface{}); ok {
					if tag, _ := m["tag"].(string); tag == "api" {
						continue
					}
					inbounds = append(inbounds, m)
				}
			}
		}
		break
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

	c.wsMu.Lock()
	err := conn.WriteJSON(msg)
	c.wsMu.Unlock()

	if err != nil {
		log.Printf("[Agent] Failed to send scan_result: %v", err)
		return
	}
	log.Printf("[Agent] Sent scan_result: xray_running=%v, inbounds=%d", xrayRunning, len(inbounds))
}
