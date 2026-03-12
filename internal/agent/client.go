package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"mmw-agent/internal/acme"
	"mmw-agent/internal/collector"
	"mmw-agent/internal/config"

	"github.com/gorilla/websocket"
)

// ConnectionMode represents the current connection mode
type ConnectionMode string

const (
	ModeWebSocket ConnectionMode = "websocket"
	ModeHTTP      ConnectionMode = "http"
	ModePull      ConnectionMode = "pull"
	ModeAuto      ConnectionMode = "auto"
)

// Client represents an agent client that connects to a master server
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

	// Connection state
	currentMode   ConnectionMode
	httpClient    *http.Client
	httpAvailable bool
	modeMu        sync.RWMutex

	// Speed calculation (from system network interface)
	lastRxBytes    int64
	lastTxBytes    int64
	lastSampleTime time.Time
	speedMu        sync.Mutex
}

// NewClient creates a new agent client
func NewClient(cfg *config.Config) *Client {
	return &Client{
		config:      cfg,
		collector:   collector.NewCollector(),
		xrayServers: cfg.XrayServers,
		stopCh:      make(chan struct{}),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		currentMode: ModePull, // Default to pull mode
	}
}

// wsHeaders returns HTTP headers for WebSocket handshake
func (c *Client) wsHeaders() http.Header {
	h := http.Header{}
	h.Set("User-Agent", config.AgentUserAgent)
	return h
}

// newRequest creates an HTTP request with standard headers (Content-Type, Authorization, User-Agent)
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
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.config.Token)
	req.Header.Set("User-Agent", config.AgentUserAgent)
	return req, nil
}

// Start starts the agent client with automatic mode selection
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

	case ModeAuto:
		fallthrough
	default:
		c.wg.Add(1)
		go c.runAutoMode(ctx)
	}
}

// Stop stops the agent client
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

// IsConnected returns whether the WebSocket is connected
func (c *Client) IsConnected() bool {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	return c.connected
}

// GetCurrentMode returns the current connection mode
func (c *Client) GetCurrentMode() ConnectionMode {
	c.modeMu.RLock()
	defer c.modeMu.RUnlock()
	return c.currentMode
}

// setCurrentMode sets the current connection mode
func (c *Client) setCurrentMode(mode ConnectionMode) {
	c.modeMu.Lock()
	defer c.modeMu.Unlock()
	c.currentMode = mode
}

// runWebSocket manages the WebSocket connection lifecycle with fallback to auto mode
func (c *Client) runWebSocket(ctx context.Context) {
	defer c.wg.Done()

	maxConsecutiveFailures := 5
	maxAuthFailures := 10
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

			// Check if this is an authentication error
			if authErr, ok := err.(*AuthError); ok {
				authFailures++
				if authErr.IsTokenInvalid() {
					log.Printf("[Agent] Authentication failed (invalid token): %v", err)
					if authFailures >= maxAuthFailures {
						log.Printf("[Agent] Too many auth failures (%d), entering sleep mode (30 min backoff)", authFailures)
						c.waitWithTrafficReport(ctx, 30*time.Minute)
						authFailures = 0
						continue
					}
				}
				// Use longer backoff for auth errors
				backoff := time.Duration(authFailures) * 30 * time.Second
				if backoff > 10*time.Minute {
					backoff = 10 * time.Minute
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

// calculateBackoff calculates the reconnection backoff duration
func (c *Client) calculateBackoff() time.Duration {
	c.reconnects++
	backoff := time.Duration(c.reconnects) * 5 * time.Second
	if backoff > 5*time.Minute {
		backoff = 5 * time.Minute
	}
	return backoff
}

// connectAndRun establishes and maintains a WebSocket connection
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

	u.Path = "/api/remote/ws"

	log.Printf("[Agent] Connecting to %s", u.String())

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
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

	return c.runMessageLoop(ctx, conn)
}

// authenticate sends the authentication message
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

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
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

// runMessageLoop handles sending traffic data, speed data, and heartbeats
func (c *Client) runMessageLoop(ctx context.Context, conn *websocket.Conn) error {
	trafficTicker := time.NewTicker(c.config.TrafficReportInterval)
	speedTicker := time.NewTicker(c.config.SpeedReportInterval)
	heartbeatTicker := time.NewTicker(30 * time.Second)
	defer trafficTicker.Stop()
	defer speedTicker.Stop()
	defer heartbeatTicker.Stop()

	msgCh := make(chan []byte, 10)
	errCh := make(chan error, 1)
	go func() {
		for {
			conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
			_, message, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			// Send message to processing channel
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

// sendTrafficData collects and sends traffic data to the master
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

// sendHeartbeat sends a heartbeat message
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

// collectLocalMetrics collects traffic metrics from local Xray servers
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

// GetStats returns the current traffic stats (for pull mode)
func (c *Client) GetStats() (*collector.XrayStats, error) {
	return c.collectLocalMetrics()
}

// GetSpeed returns the current speed data (for pull mode)
func (c *Client) GetSpeed() (uploadSpeed, downloadSpeed int64) {
	return c.collectSpeed()
}

// runAutoMode implements the three-tier fallback: WebSocket -> HTTP -> Pull
func (c *Client) runAutoMode(ctx context.Context) {
	defer c.wg.Done()
	c.runAutoModeLoop(ctx)
}

// runAutoModeLoop is the internal loop for auto mode fallback
func (c *Client) runAutoModeLoop(ctx context.Context) {
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
			continue
		}

		c.setCurrentMode(ModePull)
		log.Printf("[Agent] Falling back to pull mode - API available at /api/child/traffic and /api/child/speed")

		c.runPullModeWithTrafficReport(ctx, 30*time.Second)

		if ctx.Err() != nil {
			return
		}
		log.Printf("[Agent] Retrying higher-priority connection modes...")
	}
}

// tryWebSocketOnce attempts a single WebSocket connection test
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
	u.Path = "/api/remote/ws"

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.DialContext(ctx, u.String(), c.wsHeaders())
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

// tryHTTPOnce tests if HTTP push is available
func (c *Client) tryHTTPOnce(ctx context.Context) bool {
	u, err := url.Parse(c.config.MasterURL)
	if err != nil {
		return false
	}
	u.Path = "/api/remote/heartbeat"

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

// runHTTPReporter runs the HTTP push reporter
func (c *Client) runHTTPReporter(ctx context.Context) {
	defer c.wg.Done()
	c.setCurrentMode(ModeHTTP)
	c.runHTTPReporterLoop(ctx)
}

// runHTTPReporterLoop runs the HTTP reporting loop
func (c *Client) runHTTPReporterLoop(ctx context.Context) {
	trafficTicker := time.NewTicker(c.config.TrafficReportInterval)
	speedTicker := time.NewTicker(c.config.SpeedReportInterval)
	heartbeatTicker := time.NewTicker(30 * time.Second)
	defer trafficTicker.Stop()
	defer speedTicker.Stop()
	defer heartbeatTicker.Stop()

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

// sendTrafficHTTP sends traffic data via HTTP POST
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
	u.Path = "/api/remote/traffic"

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

// sendSpeedHTTP sends speed data via HTTP POST
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
	u.Path = "/api/remote/speed"

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

// sendHeartbeatHTTP sends heartbeat via HTTP POST
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
	u.Path = "/api/remote/heartbeat"

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

// runPullModeWithTrafficReport runs pull mode while sending traffic data to keep server online
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

// waitWithTrafficReport waits for the specified duration while sending traffic data
func (c *Client) waitWithTrafficReport(ctx context.Context, duration time.Duration) {
	if duration <= 0 {
		return
	}

	if duration > 30*time.Second {
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

// sendSpeedData sends speed data via WebSocket
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

// collectSpeed calculates the current upload and download speed from system network interface
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

// getSystemNetworkStats reads network statistics from /proc/net/dev
func (c *Client) getSystemNetworkStats() (rxBytes, txBytes int64) {
	data, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		log.Printf("[Agent] Failed to read /proc/net/dev: %v", err)
		return 0, 0
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Inter") || strings.HasPrefix(line, "face") || strings.HasPrefix(line, "lo:") {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
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

// AuthError represents an authentication error
type AuthError struct {
	Message string
	Code    string // "token_expired", "token_invalid", "server_error"
}

func (e *AuthError) Error() string {
	return "authentication failed: " + e.Message
}

// IsTokenInvalid returns true if the error indicates an invalid token
func (e *AuthError) IsTokenInvalid() bool {
	return e.Code == "token_invalid" || e.Message == "Invalid token"
}

// WebSocket message types
const (
	WSMsgTypeCertRequest = "cert_request"
	WSMsgTypeCertUpdate  = "cert_update"
	WSMsgTypeCertDeploy  = "cert_deploy"
	WSMsgTypeTokenUpdate = "token_update"
)

// WSCertRequestPayload represents a certificate request from master
type WSCertRequestPayload struct {
	CertID         int64  `json:"cert_id"`
	Domain         string `json:"domain"`
	Email          string `json:"email"`
	Provider       string `json:"provider"`
	ChallengeMode  string `json:"challenge_mode"`
	WebrootPath    string `json:"webroot_path,omitempty"`
	DNSProvider    string `json:"dns_provider,omitempty"`
	DNSCredentials string `json:"dns_credentials,omitempty"` // JSON string
	EABKid         string `json:"eab_kid,omitempty"`
	EABHmacKey     string `json:"eab_hmac_key,omitempty"`
}

// WSCertDeployPayload represents a certificate deploy command from master
type WSCertDeployPayload struct {
	Domain   string `json:"domain"`
	CertPEM  string `json:"cert_pem"`
	KeyPEM   string `json:"key_pem"`
	CertPath string `json:"cert_path"`
	KeyPath  string `json:"key_path"`
	Reload   string `json:"reload"`
}

// WSCertUpdatePayload represents a certificate update response to master
type WSCertUpdatePayload struct {
	CertID     int64     `json:"cert_id"`
	Domain     string    `json:"domain"`
	Success    bool      `json:"success"`
	CertPath   string    `json:"cert_path,omitempty"`
	KeyPath    string    `json:"key_path,omitempty"`
	CertPEM    string    `json:"cert_pem,omitempty"`
	KeyPEM     string    `json:"key_pem,omitempty"`
	IssueDate  time.Time `json:"issue_date,omitempty"`
	ExpiryDate time.Time `json:"expiry_date,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// WSTokenUpdatePayload represents a token update from master
type WSTokenUpdatePayload struct {
	ServerToken string    `json:"server_token"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// handleMessage processes incoming messages from master
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
	case WSMsgTypeCertRequest:
		var payload WSCertRequestPayload
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			log.Printf("[Agent] Failed to parse cert_request payload: %v", err)
			return
		}
		go c.handleCertRequest(conn, payload)
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
	default:
		// Ignore unknown message types
	}
}

// handleCertRequest processes a certificate request from master
func (c *Client) handleCertRequest(conn *websocket.Conn, req WSCertRequestPayload) {
	log.Printf("[Agent] Received certificate request for domain: %s", req.Domain)

	result := c.requestCertificate(req)

	// Send result back to master
	payload, _ := json.Marshal(result)
	msg := map[string]interface{}{
		"type":    WSMsgTypeCertUpdate,
		"payload": json.RawMessage(payload),
	}

	c.wsMu.Lock()
	err := conn.WriteJSON(msg)
	c.wsMu.Unlock()

	if err != nil {
		log.Printf("[Agent] Failed to send cert_update: %v", err)
	} else {
		log.Printf("[Agent] Sent cert_update for domain %s, success=%v", req.Domain, result.Success)
	}
}

// requestCertificate performs the actual certificate request using ACME
func (c *Client) requestCertificate(req WSCertRequestPayload) WSCertUpdatePayload {
	result := WSCertUpdatePayload{
		CertID: req.CertID,
		Domain: req.Domain,
	}

	opts := []acme.ClientOption{}
	if req.ChallengeMode == "webroot" && req.WebrootPath != "" {
		opts = append(opts, acme.WithWebrootDir(req.WebrootPath))
	}

	acmeClient := acme.NewClient(opts...)

	// Build V2 cert request
	certReq := acme.CertRequest{
		Email:         req.Email,
		Domain:        req.Domain,
		Provider:      req.Provider,
		ChallengeMode: req.ChallengeMode,
		WebrootPath:   req.WebrootPath,
		DNSProvider:   req.DNSProvider,
		EABKid:        req.EABKid,
		EABHmacKey:    req.EABHmacKey,
	}

	// Parse DNS credentials from JSON string
	if req.DNSCredentials != "" {
		var creds map[string]string
		if err := json.Unmarshal([]byte(req.DNSCredentials), &creds); err == nil {
			certReq.DNSCredentials = creds
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	certResult, err := acmeClient.ObtainCertificateV2(ctx, certReq)
	if err != nil {
		result.Success = false
		result.Error = err.Error()
		log.Printf("[Agent] Certificate request failed for %s: %v", req.Domain, err)
		return result
	}

	result.Success = true
	result.CertPath = certResult.CertPath
	result.KeyPath = certResult.KeyPath
	result.CertPEM = certResult.CertPEM
	result.KeyPEM = certResult.KeyPEM
	result.IssueDate = certResult.IssueDate
	result.ExpiryDate = certResult.ExpiryDate

	log.Printf("[Agent] Certificate obtained for %s, expires: %s", req.Domain, certResult.ExpiryDate)
	return result
}

// handleCertDeploy deploys a certificate received from master
func (c *Client) handleCertDeploy(payload WSCertDeployPayload) {
	log.Printf("[Agent] Received cert_deploy for domain: %s, target: %s", payload.Domain, payload.Reload)

	if err := acme.Deploy(payload.CertPEM, payload.KeyPEM, payload.CertPath, payload.KeyPath, payload.Reload); err != nil {
		log.Printf("[Agent] cert_deploy failed for %s: %v", payload.Domain, err)
	} else {
		log.Printf("[Agent] cert_deploy succeeded for %s", payload.Domain)
	}
}

// handleTokenUpdate processes a token update from master
func (c *Client) handleTokenUpdate(payload WSTokenUpdatePayload) {
	log.Printf("[Agent] Received token update from master, new token expires at %s", payload.ExpiresAt.Format(time.RFC3339))

	// Update the token in memory
	c.config.Token = payload.ServerToken

	log.Printf("[Agent] Token updated successfully in memory")
}
