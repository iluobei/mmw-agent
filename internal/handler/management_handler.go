package handler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"mmw-agent/internal/constants"
	"mmw-agent/internal/discovery"
	"mmw-agent/internal/embedded"
	"mmw-agent/internal/limiter"
	"mmw-agent/internal/util"
	"mmw-agent/internal/version"
	"mmw-agent/internal/xrayctl"
	"mmw-agent/internal/xrpc"

	"github.com/xtls/xray-core/app/proxyman/command"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf"
)

var nginxInstalling atomic.Bool

// ManageHandler 处理子端管理接口请求。
type ManageHandler struct {
	configToken         string
	configPath          string
	restartMethod       string
	restartCommand      string
	xrayMode            string
	embeddedXray        *embedded.EmbeddedXray
	embeddedMu          sync.Mutex
	onEmbeddedXrayStart func(*embedded.EmbeddedXray)
	// inboundsMu 串行化所有 manageInbound 操作(包括新的 add-client/remove-client),
	// 防止主控并发绑多个用户时:1) 配置文件 read-modify-write 撕裂;
	// 2) 主控旧 GET→remove+add 路径下相互覆盖丢 client。
	// 配置文件只有一份,锁不需要 per-inbound 粒度,直接 handler 全局即可。
	inboundsMu sync.Mutex
}

// 创建管理处理器。
func NewManageHandler(configToken, restartMethod, restartCommand string) *ManageHandler {
	return &ManageHandler{
		configToken:    configToken,
		restartMethod:  restartMethod,
		restartCommand: restartCommand,
	}
}

// SetConfigPath 设置 agent 配置文件路径，用于运行时修改配置。
func (h *ManageHandler) SetConfigPath(path string) {
	h.configPath = path
}

// SetXrayMode 设置 Xray 运行模式（embedded/external）。
func (h *ManageHandler) SetXrayMode(mode string) {
	h.xrayMode = mode
}

// OnEmbeddedXrayStart 注册 embedded xray 延迟启动后的回调。
func (h *ManageHandler) OnEmbeddedXrayStart(fn func(*embedded.EmbeddedXray)) {
	h.onEmbeddedXrayStart = fn
}

// SetEmbeddedXray 设置嵌入模式的 Xray 实例。
func (h *ManageHandler) SetEmbeddedXray(ex *embedded.EmbeddedXray) {
	h.embeddedXray = ex
}

// GetEmbeddedXray 返回当前嵌入 Xray 实例。
func (h *ManageHandler) GetEmbeddedXray() *embedded.EmbeddedXray {
	return h.embeddedXray
}

// RestartXray 使用配置的重启方式重启 xray。
func (h *ManageHandler) RestartXray() error {
	if h.xrayMode == "embedded" {
		h.embeddedMu.Lock()
		defer h.embeddedMu.Unlock()
		if h.embeddedXray != nil {
			return h.restartEmbeddedXray()
		}
		return h.lazyStartEmbeddedXray()
	}
	return xrayctl.RestartXray(h.restartMethod, h.restartCommand)
}

// restartEmbeddedXray 重启已有的 embedded xray，处理 tunnel 模式端口冲突。
// nginx 控制走 nginxIsActive/nginxStop/nginxStart helper(docker 模式自动 fallback 到 binary 直接命令)。
func (h *ManageHandler) restartEmbeddedXray() error {
	stoppedNginx := false
	if h.configNeedsPort443() {
		if nginxIsActive() {
			log.Printf("[Manage] Stopping nginx before embedded xray restart (tunnel mode)")
			_ = nginxStop()
			stoppedNginx = true
		}
	}

	err := h.embeddedXray.Restart()

	if stoppedNginx {
		log.Printf("[Manage] Restarting nginx after embedded xray restart")
		_ = nginxStart()
	}

	return err
}

// configNeedsPort443 检查当前 xray 配置是否有 inbound 监听 443 端口。
func (h *ManageHandler) configNeedsPort443() bool {
	for _, p := range constants.DefaultXrayConfigPaths {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var config struct {
			Inbounds []struct {
				Port json.Number `json:"port"`
			} `json:"inbounds"`
		}
		if json.Unmarshal(data, &config) != nil {
			continue
		}
		for _, ib := range config.Inbounds {
			if port, _ := ib.Port.Int64(); port == 443 {
				return true
			}
		}
		return false
	}
	return false
}

const fallback443Conf = `server {
    listen 443;
    listen [::]:443;
    proxy_pass 127.0.0.1:8001;
    proxy_protocol on;
}
`

func (h *ManageHandler) fallback443Path() string {
	return filepath.Join(constants.NginxPrimaryPrefixDir, "stream_servers", "xray_fallback_443.conf")
}

func (h *ManageHandler) deployFallback443() {
	path := h.fallback443Path()
	_ = os.MkdirAll(filepath.Dir(path), 0755)
	if err := os.WriteFile(path, []byte(fallback443Conf), 0644); err != nil {
		log.Printf("[Manage] Failed to deploy fallback_443: %v", err)
	} else {
		log.Printf("[Manage] Deployed fallback_443 stream config")
	}
}

func (h *ManageHandler) removeFallback443() {
	path := h.fallback443Path()
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Printf("[Manage] Failed to remove fallback_443: %v", err)
	} else {
		log.Printf("[Manage] Removed fallback_443 stream config")
	}
}

func (h *ManageHandler) reloadNginx() {
	for _, bin := range constants.NginxBinarySearchPaths {
		if p, err := exec.LookPath(bin); err == nil {
			_ = exec.Command(p, "-s", "reload").Run()
			return
		}
	}
	_ = exec.Command("systemctl", "reload", "nginx").Run()
}

// lazyStartEmbeddedXray 在 embedded 模式下延迟初始化 xray 实例。
func (h *ManageHandler) lazyStartEmbeddedXray() error {
	for _, p := range constants.DefaultXrayConfigPaths {
		if _, err := os.Stat(p); err == nil {
			log.Printf("[Manage] Lazy-starting embedded Xray with config: %s", p)
			// systemctl stop/disable xray:docker 镜像里没有外部 xray,跳过 systemctl 调用
			// (失败 noise 也省了);裸机仍然保留原行为停掉外部 xray 释放端口
			if !util.IsDocker() {
				_ = exec.Command("systemctl", "stop", "xray").Run()
				_ = exec.Command("systemctl", "disable", "xray").Run()
			}

			// 先停 nginx 释放端口（tunnel 模式 xray 需要监听 443）
			stoppedNginx := false
			if nginxIsActive() {
				log.Printf("[Manage] Stopping nginx before embedded xray start")
				_ = nginxStop()
				stoppedNginx = true
			}

			ex := embedded.New(p)
			if err := ex.Start(); err != nil {
				log.Printf("[Manage] Embedded xray start failed: %v", err)
				if stoppedNginx {
					_ = nginxStart()
				}
				return fmt.Errorf("start embedded xray: %w", err)
			}
			h.embeddedXray = ex
			if h.onEmbeddedXrayStart != nil {
				h.onEmbeddedXrayStart(ex)
			}

			if stoppedNginx {
				log.Printf("[Manage] Restarting nginx after embedded xray start")
				_ = nginxStart()
			}
			return nil
		}
	}
	return fmt.Errorf("no xray config found for embedded mode")
}

// 校验请求身份（token + User-Agent）。
func (h *ManageHandler) authenticate(r *http.Request) bool {
	// WS RPC 路径:请求由 client.go 的 handleRPCCall 构造内存 *http.Request 喂给共享 mux,
	// WS 层已经做了 securechan ECDH + token + capabilities 验证,此处放行不再做 Bearer 检查。
	// 该头由 agent 自己内部设置,真正从外部网络收到带这个头的请求也不能绕过 WS 认证 —
	// 因为 HTTP listener 上没有 WS 层,真正的 X-WS-RPC 仅出现在我们内部 ServeHTTP 调用里。
	// 不过保险起见,只要外部能伪造头,就把 listener 上的也卡住:WS RPC 入口走 net/http 内存,
	// 不经过 listener,所以判断 RemoteAddr=="ws-rpc"(由 dispatcher 强制设置)是更稳的标记。
	if r.Header.Get("X-WS-RPC") == "1" && r.RemoteAddr == "ws-rpc" {
		return true
	}

	if r.Header.Get(constants.HeaderUserAgent) != constants.AgentUserAgent {
		return false
	}

	if h.configToken == "" {
		return true
	}

	auth := r.Header.Get(constants.HeaderAuthorization)
	if auth == "" {
		auth = r.Header.Get(constants.HeaderMMRemoteToken)
	}
	if auth == "" {
		return false
	}

	if strings.HasPrefix(auth, constants.BearerPrefix) {
		token := strings.TrimPrefix(auth, constants.BearerPrefix)
		return token == h.configToken
	}

	return auth == h.configToken
}

// 输出 JSON 响应。
func writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// 输出错误响应。
func writeError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, map[string]interface{}{
		"success": false,
		"error":   message,
	})
}

// ================== 系统服务状态 ==================

// ServicesStatusResponse 表示服务状态查询响应。
type ServicesStatusResponse struct {
	Success bool           `json:"success"`
	Xray    *ServiceStatus `json:"xray,omitempty"`
	Nginx   *ServiceStatus `json:"nginx,omitempty"`
}

// ServiceStatus 表示单个服务状态。
type ServiceStatus struct {
	Installed bool   `json:"installed"`
	Running   bool   `json:"running"`
	Version   string `json:"version,omitempty"`
}

// 处理 GET /api/child/services/status。
func (h *ManageHandler) HandleServicesStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	response := ServicesStatusResponse{
		Success: true,
		Xray:    h.getXrayStatus(),
		Nginx:   h.getNginxStatus(),
	}

	writeJSON(w, http.StatusOK, response)
}

func (h *ManageHandler) getXrayStatus() *ServiceStatus {
	status := &ServiceStatus{}

	if h.xrayMode == "embedded" {
		status.Installed = true
		vs := core.VersionStatement()
		if len(vs) > 0 {
			status.Version = vs[0]
		}
		h.embeddedMu.Lock()
		status.Running = h.embeddedXray != nil && h.embeddedXray.IsRunning()
		h.embeddedMu.Unlock()
		return status
	}

	xrayPath, err := exec.LookPath("xray")
	if err != nil {
		for _, p := range constants.XrayBinarySearchPaths {
			if _, err := os.Stat(p); err == nil {
				xrayPath = p
				break
			}
		}
	}

	if xrayPath != "" {
		status.Installed = true
		cmd := exec.Command(xrayPath, "version")
		output, err := cmd.Output()
		if err == nil {
			lines := strings.Split(string(output), "\n")
			if len(lines) > 0 {
				status.Version = strings.TrimSpace(lines[0])
			}
		}
	}

	// 优先使用 systemctl 检查
	cmd := exec.Command("systemctl", "is-active", "xray")
	output, _ := cmd.Output()
	if strings.TrimSpace(string(output)) == "active" {
		status.Running = true
		return status
	}

	// 兜底：用 pgrep 检查 xray 进程
	pgrepCmd := exec.Command("pgrep", "-x", "xray")
	if err := pgrepCmd.Run(); err == nil {
		status.Running = true
		return status
	}

	// 兜底：用 ps 检查包含 "xray" 的进程
	psCmd := exec.Command("bash", "-c", "ps aux | grep -v grep | grep -E '[x]ray' | head -1")
	if output, err := psCmd.Output(); err == nil && len(strings.TrimSpace(string(output))) > 0 {
		status.Running = true
	}

	return status
}

func (h *ManageHandler) getNginxStatus() *ServiceStatus {
	status := &ServiceStatus{}

	// 先查 PATH，再查编译安装路径
	nginxPath, err := exec.LookPath("nginx")
	if err != nil {
		for _, candidate := range constants.NginxBinarySearchPaths {
			if strings.Contains(candidate, "/") {
				if _, statErr := os.Stat(candidate); statErr == nil {
					nginxPath = candidate
					err = nil
					break
				}
			}
		}
	}
	if err == nil {
		status.Installed = true
		cmd := exec.Command(nginxPath, "-v")
		output, err := cmd.CombinedOutput()
		if err == nil {
			status.Version = strings.TrimSpace(string(output))
		}
	}

	if nginxInstalling.Load() {
		status.Version = "安装中..."
	}

	// 优先使用 systemctl 检查
	cmd := exec.Command("systemctl", "is-active", "nginx")
	output, _ := cmd.Output()
	if strings.TrimSpace(string(output)) == "active" {
		status.Running = true
		return status
	}

	// 兜底：用 pgrep 检查 nginx 进程
	pgrepCmd := exec.Command("pgrep", "-x", "nginx")
	if err := pgrepCmd.Run(); err == nil {
		status.Running = true
		return status
	}

	// 兜底：用 ps 检查 nginx master 进程
	psCmd := exec.Command("bash", "-c", "ps aux | grep -v grep | grep -E 'nginx: master' | head -1")
	if output, err := psCmd.Output(); err == nil && len(strings.TrimSpace(string(output))) > 0 {
		status.Running = true
	}

	return status
}

// ================== 服务控制 ==================

// ServiceControlRequest 表示服务控制请求。
type ServiceControlRequest struct {
	Service string `json:"service"`
	Action  string `json:"action"`
}

// serviceFailureDetail 在 nginx/xray 启停失败时抓取真实失败原因:
// 先用配置测试(nginx -t / xray -test)给出具体错误(证书缺失、端口占用、配置语法等),
// 再附加 journalctl 最近日志兜底。systemctl 自身只会给"see journalctl"这类笼统提示,这里把真实原因带回主控。
func serviceFailureDetail(service string) string {
	var parts []string
	switch service {
	case "nginx":
		for _, bin := range constants.NginxBinarySearchPaths {
			if p, err := exec.LookPath(bin); err == nil {
				if out, _ := exec.Command(p, "-t").CombinedOutput(); len(strings.TrimSpace(string(out))) > 0 {
					parts = append(parts, "nginx -t: "+strings.TrimSpace(string(out)))
				}
				break
			}
		}
	case "xray":
		if p, err := exec.LookPath("xray"); err == nil {
			for _, cfg := range constants.DefaultXrayConfigPaths {
				if _, e := os.Stat(cfg); e == nil {
					if out, _ := exec.Command(p, "run", "-test", "-config", cfg).CombinedOutput(); len(strings.TrimSpace(string(out))) > 0 {
						parts = append(parts, "xray 配置检查: "+strings.TrimSpace(string(out)))
					}
					break
				}
			}
		}
	}
	if out, _ := exec.Command("journalctl", "-u", service, "-n", "12", "--no-pager", "-o", "cat").CombinedOutput(); len(strings.TrimSpace(string(out))) > 0 {
		parts = append(parts, "日志: "+strings.TrimSpace(string(out)))
	}
	return strings.Join(parts, " | ")
}

// 处理 POST /api/child/services/control。
func (h *ManageHandler) HandleServiceControl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	var req ServiceControlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Service != "xray" && req.Service != "nginx" {
		writeError(w, http.StatusBadRequest, "Invalid service. Must be 'xray' or 'nginx'")
		return
	}

	if req.Action != "start" && req.Action != "stop" && req.Action != "restart" {
		writeError(w, http.StatusBadRequest, "Invalid action. Must be 'start', 'stop', or 'restart'")
		return
	}

	if req.Service == "xray" && req.Action == "restart" {
		if err := h.RestartXray(); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("重启 xray 失败: %v %s", err, serviceFailureDetail("xray")))
			return
		}
	} else if req.Service == "xray" && req.Action == "stop" {
		// tunnel 模式：停止前恢复 nginx stream fallback，让 nginx 直接接管 443
		if h.configNeedsPort443() {
			h.deployFallback443()
			h.reloadNginx()
		}
		log.Printf("[Manage] Service xray: stop (deferred)")
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Service xray stopped successfully",
		})
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		go func() {
			time.Sleep(500 * time.Millisecond)
			if h.embeddedXray != nil {
				h.embeddedXray.Stop()
			} else {
				exec.Command("systemctl", "stop", "xray").Run()
			}
		}()
		return
	} else if req.Service == "xray" && req.Action == "start" {
		// tunnel 模式：启动前移除 nginx stream fallback，释放 443 端口
		if h.configNeedsPort443() {
			h.removeFallback443()
			h.reloadNginx()
			time.Sleep(300 * time.Millisecond)
		}
		if err := h.RestartXray(); err != nil {
			// 启动失败，恢复 fallback
			if h.configNeedsPort443() {
				h.deployFallback443()
				h.reloadNginx()
			}
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("启动 xray 失败: %v %s", err, serviceFailureDetail("xray")))
			return
		}
	} else {
		cmd := exec.Command("systemctl", req.Action, req.Service)
		output, err := cmd.CombinedOutput()
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("%s %s 失败: %v %s | %s", req.Action, req.Service, err, strings.TrimSpace(string(output)), serviceFailureDetail(req.Service)))
			return
		}
	}

	log.Printf("[Manage] Service %s: %s", req.Service, req.Action)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Service %s %sed successfully", req.Service, req.Action),
	})
}

// ================== Xray 安装 ==================

// 处理 POST /api/child/xray/install。
func (h *ManageHandler) HandleXrayInstall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	// Docker 镜像里强制 embedded 模式,xray-core 作为 Go 库编进 binary,不需要外部 xray;
	// install-release.sh 内部走 systemctl,容器里跑不通也没意义。
	if util.IsDocker() {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Docker 镜像内置 embedded xray-core,无需安装外部 xray binary",
		})
		return
	}

	log.Printf("[Manage] Installing Xray...")

	cmd := exec.Command("bash", "-c", "bash -c \"$(curl -L https://github.com/XTLS/Xray-install/raw/main/install-release.sh)\" @ install")
	cmd.Env = os.Environ()

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		log.Printf("[Manage] Xray installation failed: %v, stderr: %s", err, stderr.String())
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Installation failed: %v", err))
		return
	}

	log.Printf("[Manage] Xray installed successfully")

	// 若无配置则下发默认配置
	h.DeployDefaultXrayConfig()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Xray installed successfully",
		"output":  stdout.String(),
	})
}

// 处理 POST /api/child/xray/remove。
func (h *ManageHandler) HandleXrayRemove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	log.Printf("[Manage] Removing Xray...")

	cmd := exec.Command("bash", "-c", "bash -c \"$(curl -L https://github.com/XTLS/Xray-install/raw/main/install-release.sh)\" @ remove")
	cmd.Env = os.Environ()

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		log.Printf("[Manage] Xray removal failed: %v, stderr: %s", err, stderr.String())
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Removal failed: %v", err))
		return
	}

	log.Printf("[Manage] Xray removed successfully")

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Xray removed successfully",
		"output":  stdout.String(),
	})
}

// ================== Xray 配置 ==================

// 处理 GET/POST /api/child/xray/config。
func (h *ManageHandler) HandleXrayConfig(w http.ResponseWriter, r *http.Request) {
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.getXrayConfig(w, r)
	case http.MethodPost:
		h.setXrayConfig(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func (h *ManageHandler) getXrayConfig(w http.ResponseWriter, r *http.Request) {
	configPaths := constants.DefaultXrayConfigPaths

	var configPath string
	var content []byte
	var err error

	for _, p := range configPaths {
		content, err = os.ReadFile(p)
		if err == nil {
			configPath = p
			break
		}
	}

	// config 文件不存在时不再 404 —— 主控前端打开 Xray 配置 dialog 需要一个总能成功的入口,
	// 即便 xray 没装 / 配置被删,也应该让用户能在空 textarea 里粘贴 / 编辑配置后下发。
	// 返回空 config + 默认 path(后续 setXrayConfig 写盘也用这个 path)。
	if configPath == "" {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":  true,
			"path":     constants.DefaultXrayConfigPaths[0],
			"config":   "",
			"is_empty": true,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"path":    configPath,
		"config":  string(content),
	})
}

func (h *ManageHandler) setXrayConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Config string `json:"config"`
		Path   string `json:"path,omitempty"`
		// Force=true 跳过 xray test 验证(默认必测)。
		// 通常仅用于本地调试 / 主控判定配置确实有效需强制下发的边界场景。
		Force bool `json:"force,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	var js json.RawMessage
	if err := json.Unmarshal([]byte(req.Config), &js); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON config")
		return
	}

	configPath := req.Path
	if configPath == "" {
		configPath = constants.DefaultXrayConfigPaths[0]
	}

	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create directory: %v", err))
		return
	}

	// 即便主控推过来的整段 config 本身带重复 tag(历史 bug 残留 / 老版本主控混入),
	// 也要在写盘前去重 — xray 启动时同 tag 入站会失败。
	rawConfig := []byte(req.Config)
	if parsed := map[string]interface{}{}; json.Unmarshal(rawConfig, &parsed) == nil {
		ibRem := dedupeTaggedArrayInPlace(parsed, "inbounds")
		obRem := dedupeTaggedArrayInPlace(parsed, "outbounds")
		if ibRem > 0 || obRem > 0 {
			if deduped, err := json.MarshalIndent(parsed, "", "  "); err == nil {
				rawConfig = deduped
				log.Printf("[Manage] setXrayConfig: stripped %d duplicate inbound(s) and %d duplicate outbound(s) before write", ibRem, obRem)
			}
		}
	}

	// 写盘前用 xray test 验证(默认必测)— 阻止坏配置写入磁盘后引发 xray 启动失败 → 用户被迫 SSH 救援。
	// 外置 + 系统装了 xray 优先用命令(认 fork 字段);否则用 xray-core 库(基于 LoadJSONConfig)。
	if !req.Force {
		if testErr, output, method := runXrayTest(r.Context(), h.xrayMode, rawConfig); testErr != nil {
			log.Printf("[Manage] setXrayConfig refused write: xray test failed (%s): %v\n%s", method, testErr, output)
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"message": fmt.Sprintf("xray config test failed: %v", testErr),
				"output":  output,
				"method":  method,
			})
			return
		}
	}

	if err := os.WriteFile(configPath, rawConfig, 0644); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to write config: %v", err))
		return
	}

	log.Printf("[Manage] Xray config saved to %s", configPath)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Config saved successfully",
		"path":    configPath,
	})
}

// runXrayTest 验证一份 xray config 是否合法,返回 (err, output, method)。
// 测试不绑定端口,不和当前 running 的 xray 实例冲突。
//
// 分模式策略:
//   - mode != "embedded" + 系统装了 xray → exec `xray run -test -config <tmpfile>`(认 fork 字段、完整 -test 语义)
//   - 否则(embedded / external 但 PATH 无 xray) → embedded.TestConfigJSON(库调用 confserial.LoadJSONConfig)
//
// method 取值 "xray-cli" / "xray-library" — 前端可据此显示验证手段。
func runXrayTest(ctx context.Context, xrayMode string, rawConfig []byte) (err error, output, method string) {
	if xrayMode != "embedded" {
		if p, lookErr := exec.LookPath("xray"); lookErr == nil {
			tmpfile, terr := os.CreateTemp("", "xray-test-*.json")
			if terr == nil {
				_, _ = tmpfile.Write(rawConfig)
				_ = tmpfile.Close()
				defer os.Remove(tmpfile.Name())

				tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()
				out, cerr := exec.CommandContext(tctx, p, "run", "-test", "-config", tmpfile.Name()).CombinedOutput()
				return cerr, strings.TrimSpace(string(out)), "xray-cli"
			}
		}
	}
	if lerr := embedded.TestConfigJSON(rawConfig); lerr != nil {
		return lerr, "", "xray-library"
	}
	return nil, "", "xray-library"
}

// HandleXrayTestConfig 接收一份完整 xray config(JSON 文本),验证合法性,返回 {ok, error, output, method}。
// 主控的 Xray 配置 dialog "保存"前会调一次本接口,失败则拒绝下发,从源头杜绝写入坏 config 引发 xray 启动失败。
func (h *ManageHandler) HandleXrayTestConfig(w http.ResponseWriter, r *http.Request) {
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req struct {
		Config string `json:"config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if strings.TrimSpace(req.Config) == "" {
		writeError(w, http.StatusBadRequest, "config is empty")
		return
	}

	testErr, output, method := runXrayTest(r.Context(), h.xrayMode, []byte(req.Config))
	resp := map[string]interface{}{
		"ok":     testErr == nil,
		"output": output,
		"method": method,
	}
	if testErr != nil {
		resp["error"] = testErr.Error()
	}
	writeJSON(w, http.StatusOK, resp)
}

// ================== Xray 系统配置 ==================

// XraySystemConfig 表示 Xray 系统配置状态。
type XraySystemConfig struct {
	MetricsEnabled bool   `json:"metrics_enabled"`
	MetricsListen  string `json:"metrics_listen"`
	StatsEnabled   bool   `json:"stats_enabled"`
	GrpcEnabled    bool   `json:"grpc_enabled"`
	GrpcPort       int    `json:"grpc_port"`
}

// 处理 GET/POST /api/child/xray/system-config。
func (h *ManageHandler) HandleXraySystemConfig(w http.ResponseWriter, r *http.Request) {
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.getXraySystemConfig(w, r)
	case http.MethodPost:
		h.updateXraySystemConfig(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func (h *ManageHandler) getXraySystemConfig(w http.ResponseWriter, r *http.Request) {
	configPath := h.findXrayConfigPath()
	if configPath == "" {
		writeError(w, http.StatusNotFound, "Xray config not found")
		return
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to read config: %v", err))
		return
	}

	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Invalid JSON: %v", err))
		return
	}

	sysConfig := &XraySystemConfig{
		MetricsListen: constants.DefaultMetricsListen,
		GrpcPort:      46736,
	}

	if metrics, ok := config["metrics"].(map[string]interface{}); ok {
		sysConfig.MetricsEnabled = true
		if listen, ok := metrics["listen"].(string); ok {
			sysConfig.MetricsListen = listen
		}
	}

	if _, ok := config["stats"]; ok {
		sysConfig.StatsEnabled = true
	}

	if api, ok := config["api"].(map[string]interface{}); ok {
		if _, hasTag := api["tag"]; hasTag {
			sysConfig.GrpcEnabled = true
		}
	}

	if inbounds, ok := config["inbounds"].([]interface{}); ok {
		for _, ib := range inbounds {
			if inbound, ok := ib.(map[string]interface{}); ok {
				if tag, _ := inbound["tag"].(string); tag == "api" {
					if port, ok := inbound["port"].(float64); ok {
						sysConfig.GrpcPort = int(port)
					}
					break
				}
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"config":  sysConfig,
	})
}

func (h *ManageHandler) updateXraySystemConfig(w http.ResponseWriter, r *http.Request) {
	var req XraySystemConfig
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	configPath := h.findXrayConfigPath()
	if configPath == "" {
		writeError(w, http.StatusNotFound, "Xray config not found")
		return
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to read config: %v", err))
		return
	}

	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Invalid JSON: %v", err))
		return
	}

	if req.MetricsEnabled {
		config["metrics"] = map[string]interface{}{
			"tag":    "Metrics",
			"listen": req.MetricsListen,
		}
	} else {
		delete(config, "metrics")
	}

	if req.StatsEnabled {
		config["stats"] = map[string]interface{}{}
		config["policy"] = map[string]interface{}{
			"levels": map[string]interface{}{
				"0": map[string]interface{}{
					"handshake":         float64(5),
					"connIdle":          float64(300),
					"uplinkOnly":        float64(2),
					"downlinkOnly":      float64(2),
					"statsUserUplink":   true,
					"statsUserDownlink": true,
				},
			},
			"system": map[string]interface{}{
				"statsInboundUplink":    true,
				"statsInboundDownlink":  true,
				"statsOutboundUplink":   true,
				"statsOutboundDownlink": true,
			},
		}
	} else {
		delete(config, "stats")
		delete(config, "policy")
	}

	if req.GrpcEnabled {
		config["api"] = map[string]interface{}{
			"tag":      "api",
			"services": []interface{}{"HandlerService", "LoggerService", "StatsService", "RoutingService"},
		}

		hasAPIInbound := false
		if inbounds, ok := config["inbounds"].([]interface{}); ok {
			for i, ib := range inbounds {
				if inbound, ok := ib.(map[string]interface{}); ok {
					if tag, _ := inbound["tag"].(string); tag == "api" {
						inbound["port"] = float64(req.GrpcPort)
						inbounds[i] = inbound
						hasAPIInbound = true
						break
					}
				}
			}
			if !hasAPIInbound {
				apiInbound := map[string]interface{}{
					"tag":      "api",
					"port":     float64(req.GrpcPort),
					"listen":   constants.LocalhostIP,
					"protocol": "dokodemo-door",
					"settings": map[string]interface{}{
						"address": constants.LocalhostIP,
					},
				}
				config["inbounds"] = append([]interface{}{apiInbound}, inbounds...)
			}
		} else {
			config["inbounds"] = []interface{}{
				map[string]interface{}{
					"tag":      "api",
					"port":     float64(req.GrpcPort),
					"listen":   constants.LocalhostIP,
					"protocol": "dokodemo-door",
					"settings": map[string]interface{}{
						"address": constants.LocalhostIP,
					},
				},
			}
		}

		h.ensureAPIRoutingRule(config)
	} else {
		delete(config, "api")
		if inbounds, ok := config["inbounds"].([]interface{}); ok {
			newInbounds := make([]interface{}, 0)
			for _, ib := range inbounds {
				if inbound, ok := ib.(map[string]interface{}); ok {
					if tag, _ := inbound["tag"].(string); tag != "api" {
						newInbounds = append(newInbounds, inbound)
					}
				}
			}
			config["inbounds"] = newInbounds
		}
		h.removeAPIRoutingRule(config)
	}

	backupPath := configPath + ".backup"
	if err := os.WriteFile(backupPath, content, 0644); err != nil {
		log.Printf("[Manage] Warning: failed to backup config: %v", err)
	}

	newContent, _ := json.MarshalIndent(config, "", "    ")
	if err := os.WriteFile(configPath, newContent, 0644); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to write config: %v", err))
		return
	}

	if err := h.RestartXray(); err != nil {
		log.Printf("[Manage] Warning: failed to restart xray: %v", err)
	}

	log.Printf("[Manage] Xray system config updated: metrics=%v, stats=%v, grpc=%v",
		req.MetricsEnabled, req.StatsEnabled, req.GrpcEnabled)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "System config updated, Xray restarted",
	})
}

func (h *ManageHandler) ensureAPIRoutingRule(config map[string]interface{}) {
	routing, ok := config["routing"].(map[string]interface{})
	if !ok {
		routing = map[string]interface{}{
			"domainStrategy": "IPIfNonMatch",
			"rules":          []interface{}{},
		}
		config["routing"] = routing
	}

	rules, ok := routing["rules"].([]interface{})
	if !ok {
		rules = []interface{}{}
	}

	for _, r := range rules {
		if rule, ok := r.(map[string]interface{}); ok {
			if outboundTag, _ := rule["outboundTag"].(string); outboundTag == "api" {
				return
			}
		}
	}

	apiRule := map[string]interface{}{
		"type":        "field",
		"inboundTag":  []interface{}{"api"},
		"outboundTag": "api",
	}
	routing["rules"] = append([]interface{}{apiRule}, rules...)
}

func (h *ManageHandler) removeAPIRoutingRule(config map[string]interface{}) {
	routing, ok := config["routing"].(map[string]interface{})
	if !ok {
		return
	}

	rules, ok := routing["rules"].([]interface{})
	if !ok {
		return
	}

	newRules := make([]interface{}, 0)
	for _, r := range rules {
		if rule, ok := r.(map[string]interface{}); ok {
			if outboundTag, _ := rule["outboundTag"].(string); outboundTag != "api" {
				newRules = append(newRules, rule)
			}
		}
	}
	routing["rules"] = newRules
}

// ================== Nginx 安装 ==================

// 处理 POST /api/child/nginx/install。
func (h *ManageHandler) HandleNginxInstall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	if nginxInstalling.Load() {
		writeError(w, http.StatusConflict, "Nginx installation already in progress")
		return
	}

	var req struct {
		Domain string `json:"domain"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	// Docker 镜像已经 apt 预装 nginx + symlink 兼容 /usr/local/nginx/* 路径,
	// 不需要跑 install-nginx.sh(脚本依赖 systemd,容器里跑不通)。
	// 跟主控 installNginxLocal 早返做法一致。
	if util.IsDocker() {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Docker 镜像已预装 nginx,无需安装",
		})
		return
	}

	log.Printf("[Manage] Starting Nginx installation (async)...")
	nginxInstalling.Store(true)

	go func() {
		defer nginxInstalling.Store(false)

		cmd := exec.Command("bash", "-c",
			`curl -fsSL https://raw.githubusercontent.com/iluobei/miaomiaowuX/main/install-nginx.sh | bash`)
		cmd.Env = os.Environ()

		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			log.Printf("[Manage] Nginx installation failed: %v, stderr: %s", err, stderr.String())
			return
		}
		log.Printf("[Manage] Nginx installed successfully")

		if req.Domain != "" {
			deployNginxSSLConfig(req.Domain)
		}
	}()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Nginx installation started, please check status later",
	})
}

// 处理 POST /api/child/nginx/remove。
func (h *ManageHandler) HandleNginxRemove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	log.Printf("[Manage] Removing Nginx...")

	cmd := exec.Command("bash", "-c",
		`curl -fsSL https://raw.githubusercontent.com/iluobei/miaomiaowuX/main/uninstall-nginx.sh | bash -s -- -y`)
	cmd.Env = os.Environ()

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		log.Printf("[Manage] Nginx removal failed: %v, stderr: %s", err, stderr.String())
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Removal failed: %v", err))
		return
	}

	log.Printf("[Manage] Nginx removed successfully")

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Nginx removed successfully",
	})
}

// ================== Nginx 配置 ==================

// 处理 GET/POST /api/child/nginx/config。
func (h *ManageHandler) HandleNginxConfig(w http.ResponseWriter, r *http.Request) {
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.getNginxConfig(w, r)
	case http.MethodPost:
		h.setNginxConfig(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func (h *ManageHandler) getNginxConfig(w http.ResponseWriter, r *http.Request) {
	configPaths := constants.DefaultNginxConfigPaths

	var configPath string
	var content []byte
	var err error

	for _, p := range configPaths {
		content, err = os.ReadFile(p)
		if err == nil {
			configPath = p
			break
		}
	}

	if configPath == "" {
		writeError(w, http.StatusNotFound, "Nginx config not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"path":    configPath,
		"config":  string(content),
	})
}

func (h *ManageHandler) setNginxConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Config string `json:"config"`
		Path   string `json:"path,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	configPath := req.Path
	if configPath == "" {
		configPath = constants.DefaultNginxConfigPaths[0]
	}

	backupPath := configPath + ".bak." + time.Now().Format("20060102150405")
	if content, err := os.ReadFile(configPath); err == nil {
		os.WriteFile(backupPath, content, 0644)
	}

	if err := os.WriteFile(configPath, []byte(req.Config), 0644); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to write config: %v", err))
		return
	}

	cmd := exec.Command("nginx", "-t")
	output, err := cmd.CombinedOutput()
	if err != nil {
		if backup, err := os.ReadFile(backupPath); err == nil {
			os.WriteFile(configPath, backup, 0644)
		}
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid nginx config: %s", string(output)))
		return
	}

	log.Printf("[Manage] Nginx config saved to %s", configPath)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Config saved successfully",
		"path":    configPath,
	})
}

// ================== 系统信息 ==================

// 处理 GET /api/child/system/info。
func (h *ManageHandler) HandleSystemInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	info := map[string]interface{}{
		"success":       true,
		"agent_version": version.Version, // 主控用这个对比 GitHub latest tag 决定是否提示升级
	}

	if hostname, err := os.Hostname(); err == nil {
		info["hostname"] = hostname
	}

	if data, err := os.ReadFile("/proc/uptime"); err == nil {
		parts := strings.Fields(string(data))
		if len(parts) > 0 {
			info["uptime"] = parts[0]
		}
	}

	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		memInfo := make(map[string]string)
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])
				if key == "MemTotal" || key == "MemFree" || key == "MemAvailable" {
					memInfo[key] = value
				}
			}
		}
		info["memory"] = memInfo
	}

	if data, err := os.ReadFile("/proc/loadavg"); err == nil {
		info["loadavg"] = strings.TrimSpace(string(data))
	}

	writeJSON(w, http.StatusOK, info)
}

// ================== 配置文件管理 ==================

// ConfigFileInfo 表示配置文件条目。
type ConfigFileInfo struct {
	Name    string `json:"name"`
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	ModTime string `json:"mod_time"`
}

// 处理 xray 配置文件的列表与读写。
func (h *ManageHandler) HandleXrayConfigFiles(w http.ResponseWriter, r *http.Request) {
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	switch r.Method {
	case http.MethodGet:
		file := r.URL.Query().Get("file")
		if file != "" {
			h.getXrayConfigFile(w, r, file)
		} else {
			h.listXrayConfigFiles(w, r)
		}
	case http.MethodPut, http.MethodPost:
		h.saveXrayConfigFile(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func (h *ManageHandler) listXrayConfigFiles(w http.ResponseWriter, r *http.Request) {
	configDirs := constants.XrayConfigDirPaths

	var files []ConfigFileInfo
	var baseDir string

	for _, dir := range configDirs {
		if _, err := os.Stat(dir); err == nil {
			baseDir = dir
			entries, err := os.ReadDir(dir)
			if err != nil {
				continue
			}

			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				if !strings.HasSuffix(entry.Name(), ".json") {
					continue
				}
				info, err := entry.Info()
				if err != nil {
					continue
				}
				files = append(files, ConfigFileInfo{
					Name:    entry.Name(),
					Path:    filepath.Join(dir, entry.Name()),
					Size:    info.Size(),
					ModTime: info.ModTime().Format(time.RFC3339),
				})
			}
			break
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"base_dir": baseDir,
		"files":    files,
	})
}

func (h *ManageHandler) getXrayConfigFile(w http.ResponseWriter, r *http.Request, file string) {
	file = filepath.Clean(file)

	configDirs := []string{
		constants.XrayConfigDirPaths[0],
		constants.XrayConfigDirPaths[1],
		constants.XrayConfigDirPaths[2],
	}

	var filePath string
	for _, dir := range configDirs {
		candidate := filepath.Join(dir, file)
		if !strings.HasPrefix(candidate, dir) {
			continue
		}
		if _, err := os.Stat(candidate); err == nil {
			filePath = candidate
			break
		}
	}

	if filePath == "" {
		writeError(w, http.StatusNotFound, "File not found")
		return
	}

	content, err := os.ReadFile(filePath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to read file: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"path":    filePath,
		"content": string(content),
	})
}

func (h *ManageHandler) saveXrayConfigFile(w http.ResponseWriter, r *http.Request) {
	var req struct {
		File    string `json:"file"`
		Content string `json:"content"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.File == "" {
		writeError(w, http.StatusBadRequest, "File name required")
		return
	}

	req.File = filepath.Base(req.File)
	if !strings.HasSuffix(req.File, ".json") {
		req.File += ".json"
	}

	configDirs := []string{
		constants.XrayConfigDirPaths[0],
		constants.XrayConfigDirPaths[1],
		constants.XrayConfigDirPaths[2],
	}

	var configDir string
	for _, dir := range configDirs {
		if _, err := os.Stat(dir); err == nil {
			configDir = dir
			break
		}
	}

	if configDir == "" {
		configDir = constants.XrayConfigDirPaths[0]
		if err := os.MkdirAll(configDir, 0755); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create config directory: %v", err))
			return
		}
	}

	filePath := filepath.Join(configDir, req.File)

	if strings.HasSuffix(req.File, ".json") {
		var js json.RawMessage
		if err := json.Unmarshal([]byte(req.Content), &js); err != nil {
			writeError(w, http.StatusBadRequest, "Invalid JSON content")
			return
		}
	}

	if err := os.WriteFile(filePath, []byte(req.Content), 0644); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to write file: %v", err))
		return
	}

	log.Printf("[Manage] Xray config file saved: %s", filePath)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "File saved successfully",
		"path":    filePath,
	})
}

// 处理 nginx 配置文件的列表与读写。
func (h *ManageHandler) HandleNginxConfigFiles(w http.ResponseWriter, r *http.Request) {
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	switch r.Method {
	case http.MethodGet:
		file := r.URL.Query().Get("file")
		if file != "" {
			h.getNginxConfigFile(w, r, file)
		} else {
			h.listNginxConfigFiles(w, r)
		}
	case http.MethodPut, http.MethodPost:
		h.saveNginxConfigFile(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func (h *ManageHandler) listNginxConfigFiles(w http.ResponseWriter, r *http.Request) {
	configDirs := []struct {
		dir         string
		description string
	}{
		{constants.NginxConfigDirPaths[0], "main"},
		{constants.NginxConfigDirPaths[1], "sites-available"},
		{constants.NginxConfigDirPaths[2], "sites-enabled"},
		{constants.NginxConfigDirPaths[3], "conf.d"},
	}

	result := make(map[string][]ConfigFileInfo)

	for _, cd := range configDirs {
		if _, err := os.Stat(cd.dir); err != nil {
			continue
		}
		entries, err := os.ReadDir(cd.dir)
		if err != nil {
			continue
		}

		var files []ConfigFileInfo
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				continue
			}
			files = append(files, ConfigFileInfo{
				Name:    entry.Name(),
				Path:    filepath.Join(cd.dir, entry.Name()),
				Size:    info.Size(),
				ModTime: info.ModTime().Format(time.RFC3339),
			})
		}
		result[cd.description] = files
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"files":   result,
	})
}

func (h *ManageHandler) getNginxConfigFile(w http.ResponseWriter, r *http.Request, file string) {
	file = filepath.Clean(file)

	allowedDirs := []string{
		constants.NginxConfigDirPaths[0],
		constants.NginxConfigDirPaths[1],
		constants.NginxConfigDirPaths[2],
		constants.NginxConfigDirPaths[3],
		constants.NginxConfigDirPaths[4],
	}

	var filePath string

	if filepath.IsAbs(file) {
		for _, dir := range allowedDirs {
			if strings.HasPrefix(file, dir) {
				if _, err := os.Stat(file); err == nil {
					filePath = file
					break
				}
			}
		}
	} else {
		for _, dir := range allowedDirs {
			candidate := filepath.Join(dir, file)
			if _, err := os.Stat(candidate); err == nil {
				filePath = candidate
				break
			}
		}
	}

	if filePath == "" {
		writeError(w, http.StatusNotFound, "File not found")
		return
	}

	content, err := os.ReadFile(filePath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to read file: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"path":    filePath,
		"content": string(content),
	})
}

func (h *ManageHandler) saveNginxConfigFile(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Path == "" {
		writeError(w, http.StatusBadRequest, "File path required")
		return
	}

	req.Path = filepath.Clean(req.Path)

	allowedDirs := []string{
		constants.NginxConfigDirPaths[0],
		constants.NginxConfigDirPaths[4],
	}

	allowed := false
	for _, dir := range allowedDirs {
		if strings.HasPrefix(req.Path, dir) {
			allowed = true
			break
		}
	}

	if !allowed {
		writeError(w, http.StatusForbidden, "Path not allowed")
		return
	}

	if _, err := os.Stat(req.Path); err == nil {
		backupPath := req.Path + ".bak." + time.Now().Format("20060102150405")
		if content, err := os.ReadFile(req.Path); err == nil {
			os.WriteFile(backupPath, content, 0644)
		}
	}

	dir := filepath.Dir(req.Path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create directory: %v", err))
		return
	}

	if err := os.WriteFile(req.Path, []byte(req.Content), 0644); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to write file: %v", err))
		return
	}

	cmd := exec.Command("nginx", "-t")
	output, err := cmd.CombinedOutput()
	if err != nil {
		backupPath := req.Path + ".bak." + time.Now().Format("20060102150405")[:14]
		if backup, err := os.ReadFile(backupPath); err == nil {
			os.WriteFile(req.Path, backup, 0644)
		}
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid nginx config: %s", string(output)))
		return
	}

	log.Printf("[Manage] Nginx config file saved: %s", req.Path)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "File saved successfully",
		"path":    req.Path,
	})
}

// ================== Xray 入站管理 ==================

// InboundRequest 表示入站管理请求。
type InboundRequest struct {
	Action  string                 `json:"action"`
	Inbound map[string]interface{} `json:"inbound,omitempty"`
	Tag     string                 `json:"tag,omitempty"`
	// 仅 action=add-client/remove-client 使用:要新增 / 匹配移除的单个客户端凭据。
	// add 场景按协议放进 settings.clients(VLESS/VMess/Trojan/Hysteria/Shadowsocks)
	// 或 settings.accounts(SOCKS/HTTP);remove 场景按 matchCredentialMap 同字段匹配。
	Client map[string]interface{} `json:"client,omitempty"`
	// 仅 action=add-sniffing-exclude 使用:幂等追加到 inbound.sniffing.excludeDomains 的域名列表。
	// 典型用途:创建 vless+reality routed outbound 时,把 outbound 的伪装 SNI 加入父 inbound 的
	// sniffing 排除清单,避免 inbound 把伪装 SNI 嗅探成「真实目标」误导路由。
	Domains []string `json:"domains,omitempty"`
}

// 处理入站管理请求。
func (h *ManageHandler) HandleInbounds(w http.ResponseWriter, r *http.Request) {
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.listInbounds(w, r)
	case http.MethodPost:
		h.manageInbound(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func (h *ManageHandler) listInbounds(w http.ResponseWriter, r *http.Request) {
	// 老 mmw / 手写 xray config 里 inbound 可能没 tag,后续主控做 remove+add 时找不到 → 配置里残留旧 inbound 同端口 → xray 启动失败。
	// 不在内存里"虚拟一个 tag",而是直接把生成的 tag 写回到磁盘配置,把脏数据修干净,后续所有 remove/add 都按真实 tag 走。
	// 整个 list 流程现在的"读"步骤会带轻量"写",但只在真的有 inbound 缺 tag 时才真改文件,常规情况无副作用。
	h.promoteInboundTagsToConfig()

	configInbounds := h.getInboundsFromConfig()
	runtimeTags := h.getInboundTagsFromGRPC()
	mergedInbounds := h.mergeInbounds(configInbounds, runtimeTags)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"inbounds": mergedInbounds,
	})
}

// PromoteAllTagsOnStartup agent 启动时调用一次,把缺 tag 的 inbound/outbound 当场写回磁盘,
// 不依赖 listInbounds / listOutbounds 被调用。list 端点里的兜底逻辑保留(防止配置在 agent 启动
// 后被手动改回缺 tag 状态)。
// 同时跑一遍重复 tag 清理 — 给历史残留(主控老代码 race + persistInbound 老 bug 共同造成的
// 同 tag inbound 累加)兜底。
func (h *ManageHandler) PromoteAllTagsOnStartup() {
	h.promoteInboundTagsToConfig()
	if cfg := h.findXrayConfigPath(); cfg != "" {
		promoteOutboundTagsInFile(cfg)
	}
	h.dedupeXrayConfigTagsOnDisk()
}

// dedupeXrayConfigTagsOnDisk 是绝对兜底:把 xray config 主文件里的同 tag inbound/outbound
// 去重后写回。无论是历史残留还是任何代码路径写入,只要 agent 重启就清干净一次。
// 保留策略:**保留第一份**(早出现的通常是真实业务数据,后面的是 race 累加出来的副本)。
//
//	tag 为空的条目原样保留(可能是 vless reality 模板的中间态;不该出现但不在此处擦除)。
func (h *ManageHandler) dedupeXrayConfigTagsOnDisk() {
	configPath := h.findXrayConfigPath()
	if configPath == "" {
		return
	}
	content, err := os.ReadFile(configPath)
	if err != nil {
		return
	}
	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		return
	}
	inboundsRemoved := dedupeTaggedArrayInPlace(config, "inbounds")
	outboundsRemoved := dedupeTaggedArrayInPlace(config, "outbounds")
	if inboundsRemoved == 0 && outboundsRemoved == 0 {
		return
	}
	newContent, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		log.Printf("[Manage] dedupe xray tags: marshal failed: %v", err)
		return
	}
	if err := os.WriteFile(configPath, newContent, 0644); err != nil {
		log.Printf("[Manage] dedupe xray tags: write %s failed: %v", configPath, err)
		return
	}
	log.Printf("[Manage] dedupe xray tags: removed %d duplicate inbound(s) and %d duplicate outbound(s) from %s",
		inboundsRemoved, outboundsRemoved, configPath)
}

// dedupeTaggedArrayInPlace 在 config 里的 `key`(inbounds / outbounds)数组上去重,返回删了几条。
//
// 关键策略:**不丢用户**。同 tag 出现多份时:
//   - inbounds:把每份的 settings.clients(VLESS/VMess/Trojan/Hysteria/Shadowsocks)
//     或 settings.accounts(SOCKS/HTTP)合并去重(按 id/password/auth/user/email 主键),
//     保留第一份的 streamSettings / sniffing / port 等非 client 字段。
//     这样即使 race 产生了两份相同 tag 的 inbound,后写入的 client 不会被擦掉,套餐绑定用户不丢。
//   - outbounds:没有 client 概念,保留第一份(后写的通常是 race 副本)。
//   - 空 tag 项全部保留(模板中间态)。
func dedupeTaggedArrayInPlace(config map[string]interface{}, key string) int {
	arr, ok := config[key].([]interface{})
	if !ok {
		return 0
	}
	type slot struct {
		idx   int                    // 在 kept 里的位置
		first map[string]interface{} // 第一份(我们就地往它里面合并 clients)
	}
	seen := map[string]*slot{}
	kept := arr[:0:0]
	removed := 0
	for _, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			kept = append(kept, item)
			continue
		}
		tag, _ := m["tag"].(string)
		if tag == "" {
			kept = append(kept, item)
			continue
		}
		if existing, has := seen[tag]; has {
			if key == "inbounds" {
				mergeInboundClients(existing.first, m)
			}
			removed++
			continue
		}
		seen[tag] = &slot{idx: len(kept), first: m}
		kept = append(kept, item)
	}
	if removed > 0 {
		config[key] = kept
	}
	return removed
}

// mergeInboundClients 把 src inbound 的 settings.clients / settings.accounts 并入 dst,
// 按协议主键去重。dst 是 dedupe 后保留的第一份,src 是要丢弃的副本(但其 clients 不能丢)。
func mergeInboundClients(dst, src map[string]interface{}) {
	dstSettings, _ := dst["settings"].(map[string]interface{})
	srcSettings, _ := src["settings"].(map[string]interface{})
	if dstSettings == nil || srcSettings == nil {
		return
	}
	proto, _ := dst["protocol"].(string)
	if p, _ := src["protocol"].(string); p != "" {
		proto = p
	}
	arrKey := ""
	switch strings.ToLower(proto) {
	case "vless", "vmess", "trojan", "shadowsocks", "hysteria":
		arrKey = "clients"
	case "anytls":
		arrKey = "users"
	case "socks", "http":
		arrKey = "accounts"
	default:
		return
	}
	dstArr, _ := dstSettings[arrKey].([]interface{})
	srcArr, _ := srcSettings[arrKey].([]interface{})
	if len(srcArr) == 0 {
		return
	}
	for _, sc := range srcArr {
		sm, ok := sc.(map[string]interface{})
		if !ok {
			dstArr = append(dstArr, sc)
			continue
		}
		dup := false
		for _, dc := range dstArr {
			if dm, ok := dc.(map[string]interface{}); ok && matchClientCredential(dm, sm, proto) {
				dup = true
				break
			}
		}
		if !dup {
			dstArr = append(dstArr, sc)
		}
	}
	dstSettings[arrKey] = dstArr
	dst["settings"] = dstSettings
}

// promoteInboundTagsToConfig 扫所有 xray 配置(主 config + confdir),
// 把缺 tag 但有 protocol+port 的 inbound 原地补 tag = "<protocol>-<port>",再 marshal 写回原文件。
// 已有 tag 的不动;无 protocol/port 的(异常 inbound)也不动,避免破坏边界情况。
// 失败只记日志不阻塞 — 拿不到 tag 比 mmwx 直接 panic 要好。
func (h *ManageHandler) promoteInboundTagsToConfig() {
	paths := h.findXrayConfigInfo()
	if paths.ConfigPath != "" {
		promoteInboundTagsInFile(paths.ConfigPath)
	}
	if paths.ConfDir != "" {
		entries, err := os.ReadDir(paths.ConfDir)
		if err != nil {
			return
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(strings.ToLower(e.Name()), ".json") {
				continue
			}
			promoteInboundTagsInFile(filepath.Join(paths.ConfDir, e.Name()))
		}
	}
}

// promoteInboundTagsInFile 处理单个 xray 配置 JSON 文件,补 tag 后写回。支持两种结构:
//  1. 完整配置 {"inbounds":[...]}
//  2. 单 inbound 文件 {"protocol":...,"port":...}
func promoteInboundTagsInFile(path string) {
	content, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(content, &raw); err != nil {
		return
	}
	changed := false
	if arr, ok := raw["inbounds"].([]interface{}); ok {
		for i, ib := range arr {
			m, ok := ib.(map[string]interface{})
			if !ok {
				continue
			}
			if promoteOneInboundTag(m) {
				arr[i] = m
				changed = true
			}
		}
		if changed {
			raw["inbounds"] = arr
		}
	} else if _, ok := raw["protocol"].(string); ok {
		if promoteOneInboundTag(raw) {
			changed = true
		}
	}
	if !changed {
		return
	}
	newContent, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		log.Printf("[Manage] promote inbound tags: marshal %s failed: %v", path, err)
		return
	}
	if err := os.WriteFile(path, newContent, 0644); err != nil {
		log.Printf("[Manage] promote inbound tags: write %s failed: %v", path, err)
		return
	}
	log.Printf("[Manage] promote inbound tags: persisted generated tags into %s", path)
}

// promoteOneInboundTag 给单个 inbound map 补 tag。返回 true 表示真的改了。
func promoteOneInboundTag(m map[string]interface{}) bool {
	tag, _ := m["tag"].(string)
	if tag != "" {
		return false
	}
	protocol, _ := m["protocol"].(string)
	if protocol == "" {
		return false
	}
	port := 0
	switch p := m["port"].(type) {
	case float64:
		port = int(p)
	case int:
		port = p
	}
	if port <= 0 {
		return false
	}
	m["tag"] = fmt.Sprintf("%s-%d", protocol, port)
	return true
}

func (h *ManageHandler) getInboundsFromConfig() []map[string]interface{} {
	paths := h.findXrayConfigInfo()
	inbounds := make([]map[string]interface{}, 0)

	// 主配置文件
	if paths.ConfigPath != "" {
		inbounds = append(inbounds, readInboundsFromJSONFile(paths.ConfigPath)...)
	}

	// confdir 下的所有 *.json — xray 启动时 -confdir 会把它们合并进配置,
	// 老版 mmw 风格的服务器把每个 inbound 拆成单文件(如 Shadowsocks-25443.json)放这里。
	// 不读 confdir 会导致这些 inbound 在 listInbounds 里只能从 gRPC 拿到 tag 而拿不到 settings/port/protocol,
	// 进而让主控的 sync_inbounds 全部 skip 掉(no settings found)。
	if paths.ConfDir != "" {
		entries, err := os.ReadDir(paths.ConfDir)
		if err == nil {
			for _, e := range entries {
				if e.IsDir() || !strings.HasSuffix(strings.ToLower(e.Name()), ".json") {
					continue
				}
				inbounds = append(inbounds, readInboundsFromJSONFile(filepath.Join(paths.ConfDir, e.Name()))...)
			}
		} else {
			log.Printf("[Manage] Failed to read confdir %s: %v", paths.ConfDir, err)
		}
	}

	return inbounds
}

// readInboundsFromJSONFile 读单个 xray 风格 JSON 文件,返回其中 inbounds 数组(可能为空)。
// 支持两种结构:
//  1. 完整 xray 配置:{ "inbounds": [ ... ] }
//  2. 单 inbound 文件:{ "tag": "...", "port": ..., "protocol": ..., "settings": {...} } — 老 mmw confdir 常用
func readInboundsFromJSONFile(path string) []map[string]interface{} {
	content, err := os.ReadFile(path)
	if err != nil {
		log.Printf("[Manage] Failed to read %s: %v", path, err)
		return nil
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(content, &raw); err != nil {
		log.Printf("[Manage] Failed to parse %s: %v", path, err)
		return nil
	}

	if arr, ok := raw["inbounds"].([]interface{}); ok {
		result := make([]map[string]interface{}, 0, len(arr))
		for _, ib := range arr {
			if m, ok := ib.(map[string]interface{}); ok {
				result = append(result, m)
			}
		}
		return result
	}

	// 单 inbound 文件 — 用 protocol 字段做判定(xray inbound 必须有 protocol)
	if _, ok := raw["protocol"].(string); ok {
		return []map[string]interface{}{raw}
	}
	return nil
}

func (h *ManageHandler) getInboundTagsFromGRPC() []string {
	apiPort := h.findXrayAPIPort()
	if apiPort == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clients, err := xrpc.New(ctx, constants.LocalhostIP, uint16(apiPort))
	if err != nil {
		log.Printf("[Manage] Failed to connect to Xray gRPC: %v", err)
		return nil
	}
	defer clients.Connection.Close()

	resp, err := clients.Handler.ListInbounds(ctx, &command.ListInboundsRequest{IsOnlyTags: true})
	if err != nil {
		log.Printf("[Manage] Failed to list inbounds via gRPC: %v", err)
		return nil
	}

	tags := make([]string, 0, len(resp.Inbounds))
	for _, ib := range resp.Inbounds {
		// 过滤掉 tag="api" 和空 tag（Xray 内部入站）
		if ib.Tag != "" && ib.Tag != "api" {
			tags = append(tags, ib.Tag)
		}
	}
	return tags
}

func (h *ManageHandler) mergeInbounds(configInbounds []map[string]interface{}, runtimeTags []string) []map[string]interface{} {
	runtimeTagSet := make(map[string]bool)
	for _, tag := range runtimeTags {
		runtimeTagSet[tag] = true
	}

	configTagSet := make(map[string]bool)
	for _, ib := range configInbounds {
		if tag, ok := ib["tag"].(string); ok {
			configTagSet[tag] = true
		}
	}

	result := make([]map[string]interface{}, 0, len(configInbounds)+len(runtimeTags))
	for _, ib := range configInbounds {
		tag, _ := ib["tag"].(string)
		// 跳过 tag="api" 的入站（Xray 内部 API 入站）
		if tag == "api" {
			continue
		}
		ibCopy := make(map[string]interface{})
		for k, v := range ib {
			ibCopy[k] = v
		}
		// 如果 tag 为空，根据协议和端口生成名称
		if tag == "" {
			protocol, _ := ib["protocol"].(string)
			port := 0
			if p, ok := ib["port"].(float64); ok {
				port = int(p)
			} else if p, ok := ib["port"].(int); ok {
				port = p
			}
			if protocol != "" && port > 0 {
				ibCopy["tag"] = fmt.Sprintf("%s-%d", protocol, port)
				ibCopy["_generated_tag"] = true
			}
		}
		if runtimeTagSet[tag] {
			ibCopy["_runtime_status"] = "running"
		} else {
			ibCopy["_runtime_status"] = "not_running"
		}
		ibCopy["_source"] = "config"
		result = append(result, ibCopy)
	}

	for _, tag := range runtimeTags {
		if !configTagSet[tag] {
			result = append(result, map[string]interface{}{
				"tag":             tag,
				"_runtime_status": "running",
				"_source":         "runtime_only",
				"_warning":        "This inbound is not persisted and will be lost on restart",
			})
		}
	}

	return result
}

func (h *ManageHandler) manageInbound(w http.ResponseWriter, r *http.Request) {
	var req InboundRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	action := strings.ToLower(strings.TrimSpace(req.Action))
	if action == "" {
		action = "add"
	}

	// 全局串行化 — 见 inboundsMu 字段注释。所有 inbound CRUD(包括新的 add-client / remove-client)
	// 走同一把锁,避免并发请求间的 read-modify-write 撕裂。
	h.inboundsMu.Lock()
	defer h.inboundsMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 新的原子动作:无论 embedded / external 都共享同一份配置文件,实现可以通用化。
	switch action {
	case "add-client", "remove-client":
		h.manageInboundClient(w, ctx, action, &req)
		return
	case "add-sniffing-exclude":
		h.manageInboundSniffingExclude(w, ctx, &req)
		return
	}

	if h.xrayMode == "embedded" && h.embeddedXray != nil {
		h.manageInboundEmbedded(w, ctx, action, &req)
		return
	}

	apiPort := h.findXrayAPIPort()
	if apiPort == 0 {
		writeError(w, http.StatusInternalServerError, "Xray API not available")
		return
	}

	clients, err := xrpc.New(ctx, constants.LocalhostIP, uint16(apiPort))
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to connect to Xray: %v", err))
		return
	}
	defer clients.Connection.Close()

	switch action {
	case "add":
		if req.Inbound == nil {
			writeError(w, http.StatusBadRequest, "Inbound payload is required")
			return
		}

		if err := h.addInbound(ctx, clients.Handler, req.Inbound); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to add inbound: %v", err))
			return
		}

		if err := h.persistInbound(req.Inbound); err != nil {
			log.Printf("[Manage] Error: Failed to persist inbound to config: %v", err)
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"success": true,
				"message": "Inbound added to runtime, but failed to persist to config: " + err.Error(),
				"warning": "persist_failed",
			})
			return
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Inbound added successfully",
		})

	case "remove":
		if req.Tag == "" {
			writeError(w, http.StatusBadRequest, "Tag is required for remove action")
			return
		}

		// 尝试从运行态移除（未运行时报错可忽略）
		runtimeErr := h.removeInbound(ctx, clients.Handler, req.Tag)
		if runtimeErr != nil {
			log.Printf("[Manage] Warning: Failed to remove inbound from runtime: %v", runtimeErr)
		}

		// 从配置文件移除（主流程）
		configErr := h.removeInboundFromConfig(req.Tag)
		if configErr != nil {
			log.Printf("[Manage] Warning: Failed to remove inbound from config: %v", configErr)
		}

		// 配置文件操作成功即可视为成功（运行态移除可选）
		// 配置改动后若未重启，运行态可能还没有该入站
		if configErr != nil {
			// 配置文件操作失败
			if runtimeErr != nil {
				// 两边都失败时，判断是否只是"未找到"错误
				if strings.Contains(runtimeErr.Error(), "not enough information") {
					// Xray 返回运行态不存在该入站，这属于可接受情况
					// 仅返回配置文件错误
					writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to remove inbound from config: %v", configErr))
				} else {
					writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to remove inbound: runtime=%v, config=%v", runtimeErr, configErr))
				}
			} else {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to remove inbound from config: %v", configErr))
			}
			return
		}

		// 配置成功时，运行态报错可接受（可能尚未加载）
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Inbound removed successfully",
		})

	default:
		writeError(w, http.StatusBadRequest, "Invalid action. Must be 'add' or 'remove'")
	}
}

func (h *ManageHandler) manageInboundEmbedded(w http.ResponseWriter, ctx context.Context, action string, req *InboundRequest) {
	switch action {
	case "add":
		if req.Inbound == nil {
			writeError(w, http.StatusBadRequest, "Inbound payload is required")
			return
		}

		inboundJSON, err := json.Marshal(req.Inbound)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to marshal inbound: %v", err))
			return
		}
		inboundConfig := &conf.InboundDetourConfig{}
		if err := json.Unmarshal(inboundJSON, inboundConfig); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to parse inbound config: %v", err))
			return
		}
		rawConfig, err := inboundConfig.Build()
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to build inbound config: %v", err))
			return
		}

		if tag, ok := req.Inbound["tag"].(string); ok && tag != "" {
			_ = h.embeddedXray.RemoveInbound(tag)
		}

		if err := h.embeddedXray.AddInbound(rawConfig); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to add inbound: %v", err))
			return
		}

		if err := h.persistInbound(req.Inbound); err != nil {
			log.Printf("[Manage] Error: Failed to persist inbound to config: %v", err)
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"success": true,
				"message": "Inbound added to runtime, but failed to persist to config: " + err.Error(),
				"warning": "persist_failed",
			})
			return
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Inbound added successfully",
		})

	case "remove":
		if req.Tag == "" {
			writeError(w, http.StatusBadRequest, "Tag is required for remove action")
			return
		}

		runtimeErr := h.embeddedXray.RemoveInbound(req.Tag)
		if runtimeErr != nil {
			log.Printf("[Manage] Warning: Failed to remove inbound from runtime: %v", runtimeErr)
		}

		configErr := h.removeInboundFromConfig(req.Tag)
		if configErr != nil {
			log.Printf("[Manage] Warning: Failed to remove inbound from config: %v", configErr)
		}

		if configErr != nil {
			if runtimeErr != nil {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to remove inbound: runtime=%v, config=%v", runtimeErr, configErr))
			} else {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to remove inbound from config: %v", configErr))
			}
			return
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Inbound removed successfully",
		})

	default:
		writeError(w, http.StatusBadRequest, "Invalid action. Must be 'add' or 'remove'")
	}
}

// manageInboundClient 在 inboundsMu 锁内原子地新增/移除一个 client。
//
// 主控以前的流程是 GET(取 inbound 快照) → 在自己进程里 append client → POST remove(tag) → POST add(inbound)。
// 跨多个 HTTP 来回,并发时两个用户都基于同一份快照修改 → 后写的覆盖先写的 → 丢 client。
// 把整段操作搬到 agent 这边、由 inboundsMu 串行化,主控只需一次 POST 即可。
//
// 复用现有 manageInbound add/remove 路径作为底层运行时应用,避免重写 xray runtime 调用。
func (h *ManageHandler) manageInboundClient(w http.ResponseWriter, ctx context.Context, action string, req *InboundRequest) {
	if req.Tag == "" {
		writeError(w, http.StatusBadRequest, "tag is required")
		return
	}
	if req.Client == nil {
		writeError(w, http.StatusBadRequest, "client is required")
		return
	}

	configPath := h.findXrayConfigPath()
	if configPath == "" {
		writeError(w, http.StatusNotFound, "Xray config not found")
		return
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("read config: %v", err))
		return
	}
	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("parse config: %v", err))
		return
	}

	inbounds, _ := config["inbounds"].([]interface{})
	var target map[string]interface{}
	for _, ib := range inbounds {
		m, ok := ib.(map[string]interface{})
		if !ok {
			continue
		}
		if t, _ := m["tag"].(string); t == req.Tag {
			target = m
			break
		}
	}
	if target == nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("inbound %s not found", req.Tag))
		return
	}

	protocol, _ := target["protocol"].(string)
	settings, _ := target["settings"].(map[string]interface{})
	if settings == nil {
		settings = map[string]interface{}{}
		target["settings"] = settings
	}

	// 不同协议把客户端凭据放在 settings.clients 或 settings.accounts;协议未知则拒绝。
	var arrKey string
	switch strings.ToLower(protocol) {
	case "vless", "vmess", "trojan", "shadowsocks", "hysteria":
		arrKey = "clients"
	case "anytls":
		arrKey = "users"
	case "socks", "http":
		arrKey = "accounts"
	default:
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unsupported protocol: %s", protocol))
		return
	}
	arr, _ := settings[arrKey].([]interface{})

	switch action {
	case "add-client":
		// 幂等:已经在里面就直接返回,不写文件、不触发 runtime 重装。
		for _, c := range arr {
			if m, ok := c.(map[string]interface{}); ok && matchClientCredential(m, req.Client, protocol) {
				writeJSON(w, http.StatusOK, map[string]interface{}{
					"success": true,
					"message": "client already present (no-op)",
				})
				return
			}
		}
		arr = append(arr, req.Client)
	case "remove-client":
		filtered := arr[:0:0]
		removed := 0
		for _, c := range arr {
			if m, ok := c.(map[string]interface{}); ok && matchClientCredential(m, req.Client, protocol) {
				removed++
				continue
			}
			filtered = append(filtered, c)
		}
		if removed == 0 {
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"success": true,
				"message": "client not found (no-op)",
			})
			return
		}
		arr = filtered
	}
	settings[arrKey] = arr
	target["settings"] = settings

	// 剥掉 enumeration metadata(_generated_tag 等)再写盘,跟主控旧路径同款清理。
	for k := range target {
		if strings.HasPrefix(k, "_") {
			delete(target, k)
		}
	}

	// 写盘前最后一道兜底:把整个 inbounds 数组按 tag 去重。即使前面任何步骤手抖塞进了
	// 重复 tag 的入站,这里会被收掉,xray 永远不会因"同 tag 两份"启动失败。
	dedupeTaggedArrayInPlace(config, "inbounds")
	dedupeTaggedArrayInPlace(config, "outbounds")

	// 原子写文件:tmp + rename。同名旧文件被替换,xray reload 不会读到半截 JSON。
	newContent, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("marshal config: %v", err))
		return
	}
	tmp := configPath + ".tmp"
	if err := os.WriteFile(tmp, newContent, 0644); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("write tmp config: %v", err))
		return
	}
	if err := os.Rename(tmp, configPath); err != nil {
		os.Remove(tmp)
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("rename config: %v", err))
		return
	}

	// 运行时应用:remove 旧 inbound + add 新 inbound。在 inboundsMu 内顺序执行,
	// 不会和别的 add/remove 交错。失败也只警告 — 配置文件已经是新版,xray 下次重启就生效。
	if err := h.replaceRuntimeInbound(ctx, req.Tag, target); err != nil {
		log.Printf("[Manage] manageInboundClient: runtime apply failed (tag=%s): %v; config file already updated", req.Tag, err)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":         true,
			"message":         fmt.Sprintf("client %s persisted to config, runtime apply deferred", action),
			"runtime_warning": err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("client %sd", action),
	})
}

// manageInboundSniffingExclude 幂等追加域名到 inbound.sniffing.excludeDomains。
// 典型用途:routed outbound 是 vless+reality 时,把出站伪装 SNI 加到父 inbound 的 sniffing 排除清单,
// 避免 inbound 把伪装 SNI 嗅探成「真实目标」误导路由(用户实际访问的域名被 reality SNI 劫持)。
//
// 行为:
//   - inbound 无 sniffing 字段 → 初始化 {enabled:true, destOverride:["http","tls"], excludeDomains:[]}
//   - inbound 已有 sniffing 但无 excludeDomains 字段 → 创建空数组再追加
//   - 域名已在数组中 → 跳过(幂等);全部域名都已在 → 整轮 no-op,不写盘
//   - 调用方必须持有 inboundsMu(manageInbound 入口已加锁)
func (h *ManageHandler) manageInboundSniffingExclude(w http.ResponseWriter, ctx context.Context, req *InboundRequest) {
	if req.Tag == "" {
		writeError(w, http.StatusBadRequest, "tag is required")
		return
	}
	if len(req.Domains) == 0 {
		writeError(w, http.StatusBadRequest, "domains is required")
		return
	}

	configPath := h.findXrayConfigPath()
	if configPath == "" {
		writeError(w, http.StatusNotFound, "Xray config not found")
		return
	}
	content, err := os.ReadFile(configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("read config: %v", err))
		return
	}
	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("parse config: %v", err))
		return
	}

	inbounds, _ := config["inbounds"].([]interface{})
	var target map[string]interface{}
	for _, ib := range inbounds {
		m, ok := ib.(map[string]interface{})
		if !ok {
			continue
		}
		if t, _ := m["tag"].(string); t == req.Tag {
			target = m
			break
		}
	}
	if target == nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("inbound %s not found", req.Tag))
		return
	}

	sniffing, _ := target["sniffing"].(map[string]interface{})
	if sniffing == nil {
		sniffing = map[string]interface{}{
			"enabled":      true,
			"destOverride": []interface{}{"http", "tls"},
		}
	}
	rawExcludes, _ := sniffing["excludeDomains"].([]interface{})
	existing := make(map[string]bool, len(rawExcludes))
	for _, v := range rawExcludes {
		if s, ok := v.(string); ok {
			existing[s] = true
		}
	}
	added := 0
	for _, d := range req.Domains {
		d = strings.TrimSpace(d)
		if d == "" || existing[d] {
			continue
		}
		rawExcludes = append(rawExcludes, d)
		existing[d] = true
		added++
	}
	if added == 0 {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "all domains already present (no-op)",
		})
		return
	}
	sniffing["excludeDomains"] = rawExcludes
	target["sniffing"] = sniffing

	// 写盘前剥掉 enumeration metadata,跟 manageInboundClient 同款清理
	for k := range target {
		if strings.HasPrefix(k, "_") {
			delete(target, k)
		}
	}
	dedupeTaggedArrayInPlace(config, "inbounds")
	dedupeTaggedArrayInPlace(config, "outbounds")

	newContent, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("marshal config: %v", err))
		return
	}
	tmp := configPath + ".tmp"
	if err := os.WriteFile(tmp, newContent, 0644); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("write tmp config: %v", err))
		return
	}
	if err := os.Rename(tmp, configPath); err != nil {
		os.Remove(tmp)
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("rename config: %v", err))
		return
	}

	// 运行时应用:sniffing 改动跟 client 增删一样,需重装 inbound 才能生效
	if err := h.replaceRuntimeInbound(ctx, req.Tag, target); err != nil {
		log.Printf("[Manage] manageInboundSniffingExclude: runtime apply failed (tag=%s): %v; config file already updated", req.Tag, err)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":         true,
			"message":         fmt.Sprintf("added %d domain(s) to sniffing.excludeDomains, runtime apply deferred", added),
			"runtime_warning": err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("added %d domain(s) to sniffing.excludeDomains", added),
	})
}

// replaceRuntimeInbound 在 xray 运行态把 tag 对应的 inbound 替换成 newInbound。
// embedded 模式走内嵌 RemoveInbound + AddInbound;外置模式走 HandlerService gRPC。
// 调用方必须已经持有 inboundsMu。
func (h *ManageHandler) replaceRuntimeInbound(ctx context.Context, tag string, newInbound map[string]interface{}) error {
	if h.xrayMode == "embedded" && h.embeddedXray != nil {
		inboundJSON, err := json.Marshal(newInbound)
		if err != nil {
			return fmt.Errorf("marshal inbound: %w", err)
		}
		inboundConfig := &conf.InboundDetourConfig{}
		if err := json.Unmarshal(inboundJSON, inboundConfig); err != nil {
			return fmt.Errorf("parse inbound config: %w", err)
		}
		rawConfig, err := inboundConfig.Build()
		if err != nil {
			return fmt.Errorf("build inbound config: %w", err)
		}
		_ = h.embeddedXray.RemoveInbound(tag) // 不存在不算错
		return h.embeddedXray.AddInbound(rawConfig)
	}

	apiPort := h.findXrayAPIPort()
	if apiPort == 0 {
		return fmt.Errorf("xray API port not found")
	}
	clients, err := xrpc.New(ctx, constants.LocalhostIP, uint16(apiPort))
	if err != nil {
		return fmt.Errorf("connect xray: %w", err)
	}
	defer clients.Connection.Close()
	_ = h.removeInbound(ctx, clients.Handler, tag)
	return h.addInbound(ctx, clients.Handler, newInbound)
}

// matchClientCredential 按协议字段比对两个 client/account 是否同一身份。
// 优先看协议主键(id/password/auth/user),其次回退到 email — 路由出站清理子账号时,
// 主控只持有 email,没有完整凭据,需要这条 email 回退路径。
// 主键和 email 任一字段在两侧都非空且相等即视为命中。
func matchClientCredential(a, b map[string]interface{}, protocol string) bool {
	bothNonEmptyEq := func(k string) bool {
		av := fmt.Sprint(a[k])
		bv := fmt.Sprint(b[k])
		if av == "" || av == "<nil>" || bv == "" || bv == "<nil>" {
			return false
		}
		return av == bv
	}
	hasNonEmpty := func(m map[string]interface{}, k string) bool {
		v := fmt.Sprint(m[k])
		return v != "" && v != "<nil>"
	}
	var primaryKey string
	switch strings.ToLower(protocol) {
	case "vless", "vmess":
		primaryKey = "id"
	case "trojan", "shadowsocks", "anytls":
		primaryKey = "password"
	case "hysteria":
		primaryKey = "auth"
	case "socks", "http":
		primaryKey = "user"
	}
	// 双方都带 primary key(完整凭据) → 只看 primary key,**不** fallback email。
	// 同一 inbound 上多 client 共享 email 是合法场景(per-user 套餐绑定多客户端),
	// 老版本 email fallback 在这里会把 id 不同但 email 同的两个 client 误判为同一个,
	// add-client 时直接 no-op,新加的 client 永远进不去。
	if primaryKey != "" && hasNonEmpty(a, primaryKey) && hasNonEmpty(b, primaryKey) {
		return bothNonEmptyEq(primaryKey)
	}
	// 任一方缺 primary key — 典型是主控 removeClientFromInbound 只传 {email: ...}
	// 这种"按 email 删 client"路径需要保留,降级用 email 匹配。
	return bothNonEmptyEq("email")
}

// ================== Xray 出站管理 ==================

// OutboundRequest 表示出站管理请求。
type OutboundRequest struct {
	Action   string                 `json:"action"`
	Outbound map[string]interface{} `json:"outbound,omitempty"`
	Tag      string                 `json:"tag,omitempty"`
	// reorder action 专用:按此顺序在 xray config 文件层重排 outbounds 数组。
	// 数组首条 = xray 「无规则命中时的默认出站」,排序直接影响这条语义。
	// 运行时 xray 按 tag 命中 routing rule,数组顺序对路由本身无影响,故 reorder 不需要 xray Handler API 改动。
	Tags []string `json:"tags,omitempty"`
}

// 处理出站管理请求。
func (h *ManageHandler) HandleOutbounds(w http.ResponseWriter, r *http.Request) {
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.listOutbounds(w, r)
	case http.MethodPost:
		h.manageOutbound(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func (h *ManageHandler) listOutbounds(w http.ResponseWriter, r *http.Request) {
	configPath := h.findXrayConfigPath()
	if configPath == "" {
		writeError(w, http.StatusNotFound, "Xray config not found")
		return
	}

	// 同 listInbounds:把缺 tag 的 outbound 补 tag 写回磁盘,避免主控做 remove+add 因找不到 tag 留下重复 outbound。
	promoteOutboundTagsInFile(configPath)

	content, err := os.ReadFile(configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to read config: %v", err))
		return
	}

	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to parse config: %v", err))
		return
	}

	outbounds, _ := config["outbounds"].([]interface{})

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":   true,
		"outbounds": outbounds,
	})
}

// promoteOutboundTagsInFile 处理 xray 主配置里 outbound 缺 tag 的情况。
// 与 inbound 不同,outbound 没有 port 概念,落地 tag 用 `<protocol>-<index>`,index 是它在 outbounds 数组里的位置。
// 主要兜底场景:freedom/blackhole 等内置 outbound 用户可能手动添加时漏写 tag。
func promoteOutboundTagsInFile(configPath string) {
	content, err := os.ReadFile(configPath)
	if err != nil {
		return
	}
	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		return
	}
	arr, ok := config["outbounds"].([]interface{})
	if !ok {
		return
	}
	changed := false
	for i, ob := range arr {
		m, ok := ob.(map[string]interface{})
		if !ok {
			continue
		}
		if tag, _ := m["tag"].(string); tag != "" {
			continue
		}
		protocol, _ := m["protocol"].(string)
		if protocol == "" {
			continue
		}
		m["tag"] = fmt.Sprintf("%s-%d", protocol, i)
		arr[i] = m
		changed = true
	}
	if !changed {
		return
	}
	config["outbounds"] = arr
	newContent, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		log.Printf("[Manage] promote outbound tags: marshal %s failed: %v", configPath, err)
		return
	}
	if err := os.WriteFile(configPath, newContent, 0644); err != nil {
		log.Printf("[Manage] promote outbound tags: write %s failed: %v", configPath, err)
		return
	}
	log.Printf("[Manage] promote outbound tags: persisted generated tags into %s", configPath)
}

func (h *ManageHandler) manageOutbound(w http.ResponseWriter, r *http.Request) {
	var req OutboundRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	action := strings.ToLower(strings.TrimSpace(req.Action))
	if action == "" {
		action = "add"
	}

	apiPort := h.findXrayAPIPort()
	if apiPort == 0 {
		writeError(w, http.StatusInternalServerError, "Xray API not available")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clients, err := xrpc.New(ctx, constants.LocalhostIP, uint16(apiPort))
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to connect to Xray: %v", err))
		return
	}
	defer clients.Connection.Close()

	switch action {
	case "add":
		if req.Outbound == nil {
			writeError(w, http.StatusBadRequest, "Outbound payload is required")
			return
		}

		if err := h.addOutbound(ctx, clients.Handler, req.Outbound); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to add outbound: %v", err))
			return
		}

		if err := h.persistOutbound(req.Outbound); err != nil {
			log.Printf("[Manage] Warning: Failed to persist outbound to config: %v", err)
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Outbound added successfully",
		})

	case "remove":
		if req.Tag == "" {
			writeError(w, http.StatusBadRequest, "Tag is required for remove action")
			return
		}

		if err := h.removeOutbound(ctx, clients.Handler, req.Tag); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to remove outbound: %v", err))
			return
		}

		if err := h.removeOutboundFromConfig(req.Tag); err != nil {
			log.Printf("[Manage] Warning: Failed to remove outbound from config: %v", err)
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Outbound removed successfully",
		})

	case "update":
		// 更新 = 在 xray 运行时 remove + add(顺序不影响 routing 命中,xray 按 tag 查);
		// 持久化时按原 index 替换,保留它在 config 文件里的位置 — 这样前端列表不再被甩到末尾。
		if req.Outbound == nil {
			writeError(w, http.StatusBadRequest, "Outbound payload is required for update action")
			return
		}
		newTag, _ := req.Outbound["tag"].(string)
		if newTag == "" {
			writeError(w, http.StatusBadRequest, "Outbound tag is required for update action")
			return
		}
		// req.Tag 可选:旧 tag(若改名),为空表示 tag 没变,直接按 newTag 找位置
		oldTag := req.Tag
		if oldTag == "" {
			oldTag = newTag
		}
		if err := h.removeOutbound(ctx, clients.Handler, oldTag); err != nil {
			log.Printf("[Manage] update: remove old %q failed (continuing to add new): %v", oldTag, err)
		}
		if err := h.addOutbound(ctx, clients.Handler, req.Outbound); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to add updated outbound: %v", err))
			return
		}
		if err := h.replaceOutboundInConfig(oldTag, req.Outbound); err != nil {
			log.Printf("[Manage] Warning: Failed to replace outbound in config: %v", err)
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Outbound updated successfully",
		})

	case "reorder":
		// 按 req.Tags 顺序重排 outbounds 数组 — 只动 config 文件,不调 xray Handler API
		// (xray 运行时不依赖数组顺序;但加载配置时数组首条作为默认出站,所以重启 xray 才能让新默认生效)。
		// 不在 tags 中的 outbound 保持原相对顺序追加在末尾;tags 中找不到对应 outbound 的项跳过。
		if len(req.Tags) == 0 {
			writeError(w, http.StatusBadRequest, "tags is required for reorder action")
			return
		}
		configPath := h.findXrayConfigPath()
		if configPath == "" {
			writeError(w, http.StatusNotFound, "Xray config not found")
			return
		}
		promoteOutboundTagsInFile(configPath)
		content, err := os.ReadFile(configPath)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("read config: %v", err))
			return
		}
		var config map[string]interface{}
		if err := json.Unmarshal(content, &config); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("parse config: %v", err))
			return
		}
		obArr, _ := config["outbounds"].([]interface{})
		// tag → outbound 索引(原数组里的)
		idxByTag := make(map[string]int, len(obArr))
		for i, ob := range obArr {
			m, ok := ob.(map[string]interface{})
			if !ok {
				continue
			}
			if tag, _ := m["tag"].(string); tag != "" {
				idxByTag[tag] = i
			}
		}
		used := make(map[int]bool, len(obArr))
		reordered := make([]interface{}, 0, len(obArr))
		for _, tag := range req.Tags {
			if i, ok := idxByTag[tag]; ok && !used[i] {
				reordered = append(reordered, obArr[i])
				used[i] = true
			}
		}
		// 保留未在 tags 中的 outbound,按原相对顺序追加
		for i, ob := range obArr {
			if !used[i] {
				reordered = append(reordered, ob)
			}
		}
		config["outbounds"] = reordered
		newContent, err := json.MarshalIndent(config, "", "  ")
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("marshal config: %v", err))
			return
		}
		if err := os.WriteFile(configPath, newContent, 0644); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("write config: %v", err))
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Outbounds reordered",
		})

	default:
		writeError(w, http.StatusBadRequest, "Invalid action. Must be 'add', 'remove', 'update' or 'reorder'")
	}
}

// ================== Xray 路由管理 ==================

// RoutingRequest 表示路由管理请求。
type RoutingRequest struct {
	Action  string                 `json:"action"`
	Routing map[string]interface{} `json:"routing,omitempty"`
	Rule    map[string]interface{} `json:"rule,omitempty"`
	Index   int                    `json:"index,omitempty"`
	// 负载均衡 leastPing/leastLoad 需要的 xray 顶层观测站配置(routing.balancers 已随 Routing 透传)。
	// RawMessage 三态:缺失=保持不变;JSON null=清除该观测站;对象=写入。
	Observatory      json.RawMessage `json:"observatory,omitempty"`
	BurstObservatory json.RawMessage `json:"burstObservatory,omitempty"`
	// add_user_to_rule / remove_user_from_rule:按 marktag 定位 routing rule,增删 rule.user[] 中的 email
	Marktag   string `json:"marktag,omitempty"`
	UserEmail string `json:"user_email,omitempty"`
	// NoRestart=true 时 manageRouting 改完 config 不自动重启 xray。批量场景里主控会在循环末尾
	// 自己统一重启所有受影响服务器,agent 不需要为每条路由变更都重启一次 — N 个路由出站节点串
	// 行重启能省下 N×(1~3s),套餐绑用户感知最直接。
	NoRestart bool `json:"no_restart,omitempty"`
}

// 处理路由管理请求。
func (h *ManageHandler) HandleRouting(w http.ResponseWriter, r *http.Request) {
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.getRouting(w, r)
	case http.MethodPost:
		h.manageRouting(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func (h *ManageHandler) getRouting(w http.ResponseWriter, r *http.Request) {
	configPath := h.findXrayConfigPath()
	if configPath == "" {
		writeError(w, http.StatusNotFound, "Xray config not found")
		return
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to read config: %v", err))
		return
	}

	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to parse config: %v", err))
		return
	}

	routing, _ := config["routing"].(map[string]interface{})

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"routing": routing,
	})
}

// applyObservatory 按 RawMessage 三态把顶层 observatory/burstObservatory 写入/删除/保持。
func applyObservatory(config map[string]interface{}, key string, raw json.RawMessage) {
	if len(raw) == 0 {
		return // 缺失:保持不变
	}
	if string(raw) == "null" {
		delete(config, key) // 显式清除
		return
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err == nil {
		config[key] = obj
	}
}

func (h *ManageHandler) manageRouting(w http.ResponseWriter, r *http.Request) {
	var req RoutingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	action := strings.ToLower(strings.TrimSpace(req.Action))
	if action == "" {
		action = "set"
	}

	// inboundsMu 实际是"整个 xray config 文件"的锁。manageRouting 也读改写同一个文件,
	// 不上锁会跟并发的 manageInbound / manageInboundClient race:
	//   T1: add-client 加锁 → 读 config → 加 client → 写回 → 释放
	//   T2: manageRouting 已读到 T1 之前的旧 config → 改 routing → 写回 → 把 T1 加的 client 擦掉
	// 路由出站场景特别明显(routed 节点 = add-client + add_user_to_rule 紧挨着),
	// 主控并发绑多个用户时该 race 概率随节点数线性放大。
	h.inboundsMu.Lock()
	defer h.inboundsMu.Unlock()

	configPath := h.findXrayConfigPath()
	if configPath == "" {
		writeError(w, http.StatusNotFound, "Xray config not found")
		return
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to read config: %v", err))
		return
	}

	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to parse config: %v", err))
		return
	}

	switch action {
	case "set":
		if req.Routing == nil {
			writeError(w, http.StatusBadRequest, "Routing config is required")
			return
		}
		config["routing"] = req.Routing
		// 负载均衡 leastPing/leastLoad 的顶层观测站:对象=写入,JSON null=删除,缺失=不动。
		applyObservatory(config, "observatory", req.Observatory)
		applyObservatory(config, "burstObservatory", req.BurstObservatory)

	case "add_rule":
		if req.Rule == nil {
			writeError(w, http.StatusBadRequest, "Rule is required")
			return
		}
		routing, _ := config["routing"].(map[string]interface{})
		if routing == nil {
			routing = map[string]interface{}{}
		}
		rules, _ := routing["rules"].([]interface{})
		// 按 marktag 智能插入:1 user routed → 2 admin routed → 3 家宽/测速 warp → 4 其他。
		// 同优先级内新规则居前;不重排现有规则(只决定新 rule 落点)。
		newP := classifyRulePriority(req.Rule)
		insertAt := len(rules)
		for i, r := range rules {
			rm, ok := r.(map[string]interface{})
			if !ok {
				continue
			}
			if classifyRulePriority(rm) >= newP {
				insertAt = i
				break
			}
		}
		rules = append(rules[:insertAt], append([]interface{}{req.Rule}, rules[insertAt:]...)...)
		routing["rules"] = rules
		config["routing"] = routing

	case "remove_rule":
		routing, _ := config["routing"].(map[string]interface{})
		if routing == nil {
			writeError(w, http.StatusBadRequest, "No routing config found")
			return
		}
		rules, _ := routing["rules"].([]interface{})
		if req.Index < 0 || req.Index >= len(rules) {
			writeError(w, http.StatusBadRequest, "Invalid rule index")
			return
		}
		rules = append(rules[:req.Index], rules[req.Index+1:]...)
		routing["rules"] = rules
		config["routing"] = routing

	case "add_user_to_rule", "remove_user_from_rule":
		// 按 marktag 定位 rule,增删其 user[] 数组里的 email。
		// 主控用于 routed 节点的 routing rule 动态维护:套餐绑/退用户时不重建整条 rule,只改 user[]。
		if strings.TrimSpace(req.Marktag) == "" || strings.TrimSpace(req.UserEmail) == "" {
			writeError(w, http.StatusBadRequest, "marktag and user_email are required")
			return
		}
		routing, _ := config["routing"].(map[string]interface{})
		if routing == nil {
			writeError(w, http.StatusBadRequest, "No routing config found")
			return
		}
		rules, _ := routing["rules"].([]interface{})
		matched := -1
		for i, rule := range rules {
			rm, ok := rule.(map[string]interface{})
			if !ok {
				continue
			}
			if tag, _ := rm["marktag"].(string); tag == req.Marktag {
				matched = i
				break
			}
		}
		if matched < 0 {
			writeError(w, http.StatusNotFound, fmt.Sprintf("Rule with marktag=%s not found", req.Marktag))
			return
		}
		rule := rules[matched].(map[string]interface{})
		// user 字段可能不存在或为 nil
		users := []interface{}{}
		if existing, ok := rule["user"].([]interface{}); ok {
			users = existing
		}
		if action == "add_user_to_rule" {
			// 去重 append
			present := false
			for _, u := range users {
				if s, _ := u.(string); s == req.UserEmail {
					present = true
					break
				}
			}
			if !present {
				users = append(users, req.UserEmail)
			}
		} else {
			// remove
			filtered := users[:0]
			for _, u := range users {
				if s, _ := u.(string); s != req.UserEmail {
					filtered = append(filtered, u)
				}
			}
			users = filtered
			// 主控约定:routed 节点的 admin 占位 user 始终保留,所以这里**不会**出现空 user 数组的合法情况;
			// 但万一被外部清空,这里不主动删 rule,保留给主控决定(防止误删)。
		}
		rule["user"] = users
		rules[matched] = rule
		routing["rules"] = rules
		config["routing"] = routing

	default:
		writeError(w, http.StatusBadRequest, "Invalid action")
		return
	}

	newContent, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to marshal config: %v", err))
		return
	}

	if err := os.WriteFile(configPath, newContent, 0644); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to write config: %v", err))
		return
	}

	log.Printf("[Manage] Routing config updated")

	// xray routing 不支持动态加/改 rule(没有 gRPC API),必须重启进程才能加载新配置。
	// 默认 agent 自己重启;批量调用(主控套餐绑定 / 解绑)显式传 no_restart=true 让主控统一在末尾重启,避免 N 次串行重启。
	if req.NoRestart {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Routing updated; xray restart deferred to caller",
		})
		return
	}
	if err := h.RestartXray(); err != nil {
		log.Printf("[Manage] Routing 更新后重启 Xray 失败: %v", err)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Routing updated but xray restart failed: " + err.Error(),
			"warning": "restart_failed",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Routing updated and xray restarted",
	})
}

// ================== Batch Apply ==================

// BatchInboundClient 描述一个 add-client 操作。
type BatchInboundClient struct {
	Tag    string                 `json:"tag"`
	Client map[string]interface{} `json:"client"`
}

// BatchRoutingAddition 描述一个 routing rule add-user 操作。
// marktag 优先匹配(legacy 路径),outbound_tag fallback(auto-detected)。
type BatchRoutingAddition struct {
	Marktag     string `json:"marktag,omitempty"`
	OutboundTag string `json:"outbound_tag,omitempty"`
	UserEmail   string `json:"user_email"`
}

// BatchApplyRequest 一次性提交多个 inbound add-client + routing rule add-user 操作。
// 在 inboundsMu 锁内单次读 config + 单次写盘 + per-inbound runtime apply 完成。
type BatchApplyRequest struct {
	InboundClients       []BatchInboundClient   `json:"inbound_clients,omitempty"`
	RoutingUserAdditions []BatchRoutingAddition `json:"routing_user_additions,omitempty"`
	// NoRestart=true 时改完 routing 不自动重启 xray(主控统一末尾重启)。
	NoRestart bool `json:"no_restart,omitempty"`
}

// BatchApplyResult 返回每条改动的结果,以及最终的运行时/重启状态。
type BatchApplyResult struct {
	Success         bool     `json:"success"`
	InboundResults  []string `json:"inbound_results,omitempty"`  // 与 InboundClients 一一对应,"ok"/"err: xxx"
	RoutingResults  []string `json:"routing_results,omitempty"`  // 与 RoutingUserAdditions 一一对应
	RuntimeWarnings []string `json:"runtime_warnings,omitempty"` // replaceRuntimeInbound 失败提示
	RoutingChanged  bool     `json:"routing_changed,omitempty"`  // 是否真改了 routing(决定 caller 是否需要重启)
	RestartedXray   bool     `json:"restarted_xray,omitempty"`
	Message         string   `json:"message,omitempty"`
}

// applyAddClientToConfig 在内存中的 xray config 上加 client(幂等),不写盘。
// 返回该 inbound 的 map(供 caller 后续 replaceRuntimeInbound 用),已存在或未变也返回该 map。
func applyAddClientToConfig(config map[string]interface{}, tag string, client map[string]interface{}) (map[string]interface{}, bool, error) {
	inbounds, _ := config["inbounds"].([]interface{})
	for _, ib := range inbounds {
		m, ok := ib.(map[string]interface{})
		if !ok {
			continue
		}
		if t, _ := m["tag"].(string); t != tag {
			continue
		}
		protocol, _ := m["protocol"].(string)
		settings, _ := m["settings"].(map[string]interface{})
		if settings == nil {
			settings = map[string]interface{}{}
			m["settings"] = settings
		}
		var arrKey string
		switch strings.ToLower(protocol) {
		case "vless", "vmess", "trojan", "shadowsocks", "hysteria":
			arrKey = "clients"
		case "anytls":
			arrKey = "users"
		case "socks", "http":
			arrKey = "accounts"
		default:
			return nil, false, fmt.Errorf("unsupported protocol: %s", protocol)
		}
		arr, _ := settings[arrKey].([]interface{})
		// 幂等
		for _, c := range arr {
			if cm, ok := c.(map[string]interface{}); ok && matchClientCredential(cm, client, protocol) {
				return m, false, nil // 已存在,inbound 未变
			}
		}
		arr = append(arr, client)
		settings[arrKey] = arr
		return m, true, nil
	}
	return nil, false, fmt.Errorf("inbound %s not found", tag)
}

// applyAddUserToRouteInConfig 在 config["routing"].rules 中按 marktag / outboundTag 找匹配 rule,
// 把 userEmail 去重 append 到 rule.user[]。
// 返回是否真的改了(false = 已存在,跳过)。
func applyAddUserToRouteInConfig(config map[string]interface{}, marktag, outboundTag, userEmail string) (bool, error) {
	if strings.TrimSpace(userEmail) == "" {
		return false, fmt.Errorf("user_email is empty")
	}
	if strings.TrimSpace(marktag) == "" && strings.TrimSpace(outboundTag) == "" {
		return false, fmt.Errorf("marktag or outbound_tag required")
	}
	routing, _ := config["routing"].(map[string]interface{})
	if routing == nil {
		return false, fmt.Errorf("no routing config")
	}
	rules, _ := routing["rules"].([]interface{})
	matched := -1
	for i, ru := range rules {
		rm, ok := ru.(map[string]interface{})
		if !ok {
			continue
		}
		if marktag != "" {
			if mt, _ := rm["marktag"].(string); mt == marktag {
				matched = i
				break
			}
		} else if outboundTag != "" {
			if t, _ := rm["outboundTag"].(string); t == outboundTag {
				matched = i
				break
			}
		}
	}
	if matched < 0 {
		return false, fmt.Errorf("rule not found marktag=%q outboundTag=%q", marktag, outboundTag)
	}
	rule := rules[matched].(map[string]interface{})
	users := []interface{}{}
	if existing, ok := rule["user"].([]interface{}); ok {
		users = existing
	}
	for _, u := range users {
		if s, _ := u.(string); s == userEmail {
			return false, nil // 已存在,幂等
		}
	}
	rule["user"] = append(users, userEmail)
	rules[matched] = rule
	routing["rules"] = rules
	config["routing"] = routing
	return true, nil
}

// HandleBatchApply 一次性提交多个 inbound 加 client + routing rule 加 user。
// 在 inboundsMu 锁内单次读 config → 内存修改 → 单次写盘 → per-inbound runtime apply。
// 用于套餐绑用户:同台 server 上多个 routed 节点的所有改动合并成 1 次 round-trip。
func (h *ManageHandler) HandleBatchApply(w http.ResponseWriter, r *http.Request) {
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	var req BatchApplyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if len(req.InboundClients) == 0 && len(req.RoutingUserAdditions) == 0 {
		writeJSON(w, http.StatusOK, BatchApplyResult{Success: true, Message: "nothing to apply"})
		return
	}

	h.inboundsMu.Lock()
	defer h.inboundsMu.Unlock()

	configPath := h.findXrayConfigPath()
	if configPath == "" {
		writeError(w, http.StatusNotFound, "Xray config not found")
		return
	}
	content, err := os.ReadFile(configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("read config: %v", err))
		return
	}
	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("parse config: %v", err))
		return
	}

	result := BatchApplyResult{
		InboundResults: make([]string, len(req.InboundClients)),
		RoutingResults: make([]string, len(req.RoutingUserAdditions)),
	}
	// 受影响 inbound 收集起来:写盘后逐个 replaceRuntimeInbound,而不是每加一个 client 都 reload。
	affectedInbounds := map[string]map[string]interface{}{}
	inboundChanged := false

	for i, item := range req.InboundClients {
		if item.Tag == "" || item.Client == nil {
			result.InboundResults[i] = "err: tag and client required"
			continue
		}
		target, changed, err := applyAddClientToConfig(config, item.Tag, item.Client)
		if err != nil {
			result.InboundResults[i] = "err: " + err.Error()
			continue
		}
		if changed {
			inboundChanged = true
			affectedInbounds[item.Tag] = target
			result.InboundResults[i] = "ok"
		} else {
			result.InboundResults[i] = "ok (no-op)"
		}
	}

	routingChanged := false
	for i, item := range req.RoutingUserAdditions {
		changed, err := applyAddUserToRouteInConfig(config, item.Marktag, item.OutboundTag, item.UserEmail)
		if err != nil {
			result.RoutingResults[i] = "err: " + err.Error()
			continue
		}
		if changed {
			routingChanged = true
			result.RoutingResults[i] = "ok"
		} else {
			result.RoutingResults[i] = "ok (no-op)"
		}
	}
	result.RoutingChanged = routingChanged

	if !inboundChanged && !routingChanged {
		// 全是幂等 no-op,跳过写盘和重启
		result.Success = true
		result.Message = "all no-op, config unchanged"
		writeJSON(w, http.StatusOK, result)
		return
	}

	// 写盘前最后一道兜底:把整个 inbounds/outbounds 数组按 tag 去重。
	dedupeTaggedArrayInPlace(config, "inbounds")
	dedupeTaggedArrayInPlace(config, "outbounds")

	newContent, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("marshal config: %v", err))
		return
	}
	tmp := configPath + ".tmp"
	if err := os.WriteFile(tmp, newContent, 0644); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("write tmp config: %v", err))
		return
	}
	if err := os.Rename(tmp, configPath); err != nil {
		os.Remove(tmp)
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("rename config: %v", err))
		return
	}

	// 运行时应用:对每个改过的 inbound 做一次 replaceRuntimeInbound。失败只警告,不打断。
	if inboundChanged {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for tag, target := range affectedInbounds {
			if err := h.replaceRuntimeInbound(ctx, tag, target); err != nil {
				log.Printf("[BatchApply] runtime apply tag=%s failed: %v", tag, err)
				result.RuntimeWarnings = append(result.RuntimeWarnings, fmt.Sprintf("%s: %v", tag, err))
			}
		}
	}

	// routing 改动需要 xray restart 才能生效;NoRestart=true 时由 caller 统一末尾重启。
	if routingChanged && !req.NoRestart {
		if err := h.RestartXray(); err != nil {
			log.Printf("[BatchApply] restart xray failed: %v", err)
			result.Message = "config persisted, xray restart failed: " + err.Error()
		} else {
			result.RestartedXray = true
		}
	}

	result.Success = true
	if result.Message == "" {
		result.Message = fmt.Sprintf("applied %d inbound + %d routing changes", len(affectedInbounds), len(req.RoutingUserAdditions))
	}
	writeJSON(w, http.StatusOK, result)
}

// ================== 辅助函数 ==================

func (h *ManageHandler) findXrayConfigPath() string {
	// embedded 模式下,Discover 找不到运行中的外置 xray 进程(没了)/systemd unit 指的是已归档路径(无效),
	// 会回退到 static paths;但更直接的是用 embedded 的标准路径,跟启动时一致。
	if h.xrayMode == "embedded" {
		if p := constants.DefaultXrayConfigPaths[0]; fileExists(p) {
			return p
		}
	}
	p := discovery.Discover()
	if p.ConfigPath != "" {
		return p.ConfigPath
	}
	return ""
}

func (h *ManageHandler) findXrayConfigInfo() discovery.XrayPaths {
	if h.xrayMode == "embedded" {
		p := constants.DefaultXrayConfigPaths[0]
		if fileExists(p) {
			return discovery.XrayPaths{ConfigPath: p}
		}
	}
	return discovery.Discover()
}

func fileExists(p string) bool {
	if p == "" {
		return false
	}
	_, err := os.Stat(p)
	return err == nil
}

func (h *ManageHandler) findXrayAPIPort() int {
	configPath := h.findXrayConfigPath()
	if configPath == "" {
		return 0
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		return 0
	}

	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		return 0
	}

	inbounds, ok := config["inbounds"].([]interface{})
	if !ok {
		return 0
	}

	for _, ib := range inbounds {
		inbound, ok := ib.(map[string]interface{})
		if !ok {
			continue
		}
		tag, _ := inbound["tag"].(string)
		if tag == "api" {
			port, ok := inbound["port"].(float64)
			if ok {
				return int(port)
			}
		}
	}

	return 10085
}

func (h *ManageHandler) addInbound(ctx context.Context, handlerClient command.HandlerServiceClient, inbound map[string]interface{}) error {
	inboundJSON, err := json.Marshal(inbound)
	if err != nil {
		return fmt.Errorf("failed to marshal inbound: %w", err)
	}

	inboundConfig := &conf.InboundDetourConfig{}
	if err := json.Unmarshal(inboundJSON, inboundConfig); err != nil {
		return fmt.Errorf("failed to unmarshal inbound config: %w", err)
	}

	rawConfig, err := inboundConfig.Build()
	if err != nil {
		return fmt.Errorf("failed to build inbound config: %w", err)
	}

	if tag, ok := inbound["tag"].(string); ok && tag != "" {
		_, _ = handlerClient.RemoveInbound(ctx, &command.RemoveInboundRequest{
			Tag: tag,
		})
	}

	_, err = handlerClient.AddInbound(ctx, &command.AddInboundRequest{
		Inbound: rawConfig,
	})
	return err
}

func (h *ManageHandler) removeInbound(ctx context.Context, handlerClient command.HandlerServiceClient, tag string) error {
	_, err := handlerClient.RemoveInbound(ctx, &command.RemoveInboundRequest{
		Tag: tag,
	})
	return err
}

func (h *ManageHandler) addOutbound(ctx context.Context, handlerClient command.HandlerServiceClient, outbound map[string]interface{}) error {
	outboundJSON, err := json.Marshal(outbound)
	if err != nil {
		return fmt.Errorf("failed to marshal outbound: %w", err)
	}

	outboundConfig := &conf.OutboundDetourConfig{}
	if err := json.Unmarshal(outboundJSON, outboundConfig); err != nil {
		return fmt.Errorf("failed to unmarshal outbound config: %w", err)
	}

	rawConfig, err := outboundConfig.Build()
	if err != nil {
		return fmt.Errorf("failed to build outbound config: %w", err)
	}

	_, err = handlerClient.AddOutbound(ctx, &command.AddOutboundRequest{
		Outbound: rawConfig,
	})
	return err
}

func (h *ManageHandler) removeOutbound(ctx context.Context, handlerClient command.HandlerServiceClient, tag string) error {
	_, err := handlerClient.RemoveOutbound(ctx, &command.RemoveOutboundRequest{
		Tag: tag,
	})
	return err
}

func (h *ManageHandler) persistInbound(inbound map[string]interface{}) error {
	configPath := h.findXrayConfigPath()
	if configPath == "" {
		return fmt.Errorf("config file not found")
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}

	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	// 按 tag 去重:同 tag 已存在则替换,不存在再 append。
	// 防止主控老代码 `GET → modify → POST remove + POST add` 在并发下因竞态丢 remove 而 append 出多份(case:套餐绑用户的 4 个 POST 在 inboundsMu 内序列化后,
	//   M1.remove → 0
	//   M2.remove → 0
	//   M1.add    → 1
	//   M2.add    → 持续 append 出 2 份
	// 历史上 us-a / Akko SJC 的 vless-443 被复制成 3 份就是这个原因)。
	// agent inboundsMu 已经能挡跨连接的并发,但 persistInbound 自己幂等才是最后一道兜底。
	newTag, _ := inbound["tag"].(string)
	inbounds, _ := config["inbounds"].([]interface{})
	if newTag != "" {
		replaced := false
		for i, ib := range inbounds {
			m, ok := ib.(map[string]interface{})
			if !ok {
				continue
			}
			if t, _ := m["tag"].(string); t == newTag {
				inbounds[i] = inbound
				replaced = true
				break
			}
		}
		if !replaced {
			inbounds = append(inbounds, inbound)
		}
	} else {
		// tag 缺失的退化路径(理论上 listInbounds 的 promote 应该把它补上,此处只兜底)
		inbounds = append(inbounds, inbound)
	}
	config["inbounds"] = inbounds

	// 最后一道兜底:整组重新 dedupe,无论上面 if/else 路径走了哪条,
	// 写盘前同 tag 数组里最多保留一份。
	dedupeTaggedArrayInPlace(config, "inbounds")
	dedupeTaggedArrayInPlace(config, "outbounds")

	newContent, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	return os.WriteFile(configPath, newContent, 0644)
}

func (h *ManageHandler) removeInboundFromConfig(tag string) error {
	configPath := h.findXrayConfigPath()
	if configPath == "" {
		return fmt.Errorf("config file not found")
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}

	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	inbounds, _ := config["inbounds"].([]interface{})
	var newInbounds []interface{}
	for _, ib := range inbounds {
		inbound, ok := ib.(map[string]interface{})
		if !ok {
			newInbounds = append(newInbounds, ib)
			continue
		}
		ibTag, _ := inbound["tag"].(string)
		if ibTag == tag {
			continue // 精确匹配 → 删
		}
		// listInbounds 会给 tag 缺失的 inbound 虚拟一个 `<protocol>-<port>` tag 让前端能引用,
		// remove 时也要识别这种虚拟 tag,否则原 inbound 永远删不掉 → mmwx 再 add 一份 → 同端口两份 inbound → xray 启动失败
		if ibTag == "" {
			proto, _ := inbound["protocol"].(string)
			port := 0
			switch p := inbound["port"].(type) {
			case float64:
				port = int(p)
			case int:
				port = p
			}
			if proto != "" && port > 0 && tag == fmt.Sprintf("%s-%d", proto, port) {
				continue
			}
		}
		newInbounds = append(newInbounds, ib)
	}
	config["inbounds"] = newInbounds

	newContent, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	return os.WriteFile(configPath, newContent, 0644)
}

func (h *ManageHandler) persistOutbound(outbound map[string]interface{}) error {
	configPath := h.findXrayConfigPath()
	if configPath == "" {
		return fmt.Errorf("config file not found")
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}

	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	outbounds, _ := config["outbounds"].([]interface{})
	outbounds = append(outbounds, outbound)
	config["outbounds"] = outbounds

	newContent, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	return os.WriteFile(configPath, newContent, 0644)
}

// replaceOutboundInConfig 按 oldTag 找到现有 outbound,在原位置原地替换为新 outbound;
// 找不到则追加在末尾(保持与 add 行为一致)。这是为了让 UI 编辑保存后,出站不会被甩到列表末尾。
func (h *ManageHandler) replaceOutboundInConfig(oldTag string, outbound map[string]interface{}) error {
	configPath := h.findXrayConfigPath()
	if configPath == "" {
		return fmt.Errorf("config file not found")
	}
	content, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}
	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	outbounds, _ := config["outbounds"].([]interface{})
	replaced := false
	for i, ob := range outbounds {
		om, ok := ob.(map[string]interface{})
		if !ok {
			continue
		}
		if t, _ := om["tag"].(string); t == oldTag {
			outbounds[i] = outbound
			replaced = true
			break
		}
	}
	if !replaced {
		outbounds = append(outbounds, outbound)
	}
	config["outbounds"] = outbounds
	newContent, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	return os.WriteFile(configPath, newContent, 0644)
}

func (h *ManageHandler) removeOutboundFromConfig(tag string) error {
	configPath := h.findXrayConfigPath()
	if configPath == "" {
		return fmt.Errorf("config file not found")
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}

	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	outbounds, _ := config["outbounds"].([]interface{})
	var newOutbounds []interface{}
	for _, ob := range outbounds {
		outbound, ok := ob.(map[string]interface{})
		if !ok {
			newOutbounds = append(newOutbounds, ob)
			continue
		}
		obTag, _ := outbound["tag"].(string)
		if obTag != tag {
			newOutbounds = append(newOutbounds, ob)
		}
	}
	config["outbounds"] = newOutbounds

	newContent, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	return os.WriteFile(configPath, newContent, 0644)
}

// ================== 扫描 ==================

// ScanResponse 表示扫描接口响应。
type ScanResponse struct {
	Success             bool                     `json:"success"`
	Message             string                   `json:"message"`
	XrayRunning         bool                     `json:"xray_running"`
	XrayVersion         string                   `json:"xray_version,omitempty"`
	APIPort             int                      `json:"api_port,omitempty"`
	ConfigPath          string                   `json:"config_path,omitempty"`
	Inbounds            []map[string]interface{} `json:"inbounds,omitempty"`
	ConfigModified      bool                     `json:"config_modified,omitempty"`
	ConfigAddedSections []string                 `json:"config_added_sections,omitempty"`
}

// 处理 POST /api/child/scan。
func (h *ManageHandler) HandleScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	log.Printf("[Manage] Scanning for Xray process...")

	configResult := h.EnsureXrayConfig()

	response := ScanResponse{
		Success: true,
		Message: "Scan completed",
	}

	if configResult.Modified {
		response.ConfigModified = true
		response.ConfigAddedSections = configResult.AddedSections
		log.Printf("[Manage] Xray config auto-completed, added sections: %v", configResult.AddedSections)
		if err := h.RestartXray(); err != nil {
			log.Printf("[Manage] Failed to restart xray after config update: %v", err)
		} else {
			log.Printf("[Manage] Xray restarted after config update")
			time.Sleep(1 * time.Second)
		}
	} else if configResult.Error != "" {
		log.Printf("[Manage] Xray config check warning: %s", configResult.Error)
	}

	xrayStatus := h.getXrayStatus()
	if xrayStatus != nil {
		response.XrayRunning = xrayStatus.Running
		response.XrayVersion = xrayStatus.Version
	}

	configPath := h.findXrayConfigPath()
	if configPath != "" {
		response.ConfigPath = configPath
		response.APIPort = h.findXrayAPIPort()

		content, err := os.ReadFile(configPath)
		if err == nil {
			var config map[string]interface{}
			if json.Unmarshal(content, &config) == nil {
				if inbounds, ok := config["inbounds"].([]interface{}); ok {
					for _, ib := range inbounds {
						if inbound, ok := ib.(map[string]interface{}); ok {
							if tag, _ := inbound["tag"].(string); tag == "api" {
								continue
							}
							response.Inbounds = append(response.Inbounds, inbound)
						}
					}
				}
			}
		}
	}

	if response.XrayRunning {
		response.Message = fmt.Sprintf("Xray is running, found %d inbound(s)", len(response.Inbounds))
		if response.ConfigModified {
			response.Message += fmt.Sprintf(", config updated: added %v", response.ConfigAddedSections)
		}
	} else if xrayStatus != nil && xrayStatus.Installed {
		response.Message = "Xray is installed but not running"
	} else {
		response.Message = "Xray is not installed"
	}

	log.Printf("[Manage] Scan result: %s", response.Message)

	writeJSON(w, http.StatusOK, response)
}

// ================== Xray 配置自动补全 ==================

// EnsureXrayConfigResult 表示配置检查结果。
type EnsureXrayConfigResult struct {
	ConfigPath    string   `json:"config_path"`
	Modified      bool     `json:"modified"`
	AddedSections []string `json:"added_sections,omitempty"`
	Error         string   `json:"error,omitempty"`
}

// 检查并补全 Xray 配置。
func (h *ManageHandler) EnsureXrayConfig() *EnsureXrayConfigResult {
	result := &EnsureXrayConfigResult{}

	configPath := h.findXrayConfigPath()
	if configPath == "" {
		result.Error = "Xray config not found"
		return result
	}
	result.ConfigPath = configPath

	content, err := os.ReadFile(configPath)
	if err != nil {
		result.Error = fmt.Sprintf("Failed to read config: %v", err)
		return result
	}

	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		result.Error = fmt.Sprintf("Invalid JSON: %v", err)
		return result
	}

	modified := false

	if _, ok := config["api"]; !ok {
		config["api"] = map[string]interface{}{
			"tag":      "api",
			"services": []interface{}{"HandlerService", "LoggerService", "StatsService", "RoutingService"},
		}
		result.AddedSections = append(result.AddedSections, "api")
		modified = true
	}

	if _, ok := config["stats"]; !ok {
		config["stats"] = map[string]interface{}{}
		result.AddedSections = append(result.AddedSections, "stats")
		modified = true
	}

	if !h.hasValidPolicy(config) {
		config["policy"] = h.getTemplatePolicy()
		result.AddedSections = append(result.AddedSections, "policy")
		modified = true
	}

	if !h.hasAPIInbound(config) {
		h.addAPIInbound(config)
		result.AddedSections = append(result.AddedSections, "api_inbound")
		modified = true
	}

	if !h.hasAPIRoutingRule(config) {
		h.addAPIRoutingRule(config)
		result.AddedSections = append(result.AddedSections, "api_routing_rule")
		modified = true
	}

	if !h.hasRequiredOutbounds(config) {
		h.addRequiredOutbounds(config)
		result.AddedSections = append(result.AddedSections, "outbounds")
		modified = true
	}

	if _, ok := config["metrics"]; !ok {
		config["metrics"] = map[string]interface{}{
			"tag":    "Metrics",
			"listen": "127.0.0.1:38889",
		}
		result.AddedSections = append(result.AddedSections, "metrics")
		modified = true
	}

	if _, ok := config["log"]; !ok {
		config["log"] = map[string]interface{}{
			"loglevel": "error",
		}
		result.AddedSections = append(result.AddedSections, "log")
		modified = true
	}

	if h.ensureRoutingRules(config) {
		result.AddedSections = append(result.AddedSections, "routing_rules")
		modified = true
	}

	// 8a9f8c9 移除 fix_openai 路由规则,但残留在老 agent 的 config.json 里 —
	// 启动期顺手清掉,跟主控的 migration 双端对齐,避免触发"配置漂移"提示。
	if h.removeDeprecatedRoutingRules(config) {
		result.AddedSections = append(result.AddedSections, "removed_deprecated_routing_rules")
		modified = true
	}

	// 兜底 tag 去重(这里是 agent 每次启动 + 任何配置写入都会经过的检查路径,挂一道保险)。
	if removed := dedupeTaggedArrayInPlace(config, "inbounds"); removed > 0 {
		result.AddedSections = append(result.AddedSections, fmt.Sprintf("dedupe_inbounds_removed=%d", removed))
		modified = true
	}
	if removed := dedupeTaggedArrayInPlace(config, "outbounds"); removed > 0 {
		result.AddedSections = append(result.AddedSections, fmt.Sprintf("dedupe_outbounds_removed=%d", removed))
		modified = true
	}

	if modified {
		backupPath := configPath + ".backup"
		if err := os.WriteFile(backupPath, content, 0644); err != nil {
			log.Printf("[Manage] Warning: failed to backup config: %v", err)
		}

		newContent, _ := json.MarshalIndent(config, "", "    ")
		if err := os.WriteFile(configPath, newContent, 0644); err != nil {
			result.Error = fmt.Sprintf("Failed to write config: %v", err)
			return result
		}
		result.Modified = true
		log.Printf("[Manage] Xray config updated, added: %v", result.AddedSections)
	}

	return result
}

func (h *ManageHandler) hasValidPolicy(config map[string]interface{}) bool {
	policy, ok := config["policy"].(map[string]interface{})
	if !ok {
		return false
	}

	levels, ok := policy["levels"].(map[string]interface{})
	if !ok {
		return false
	}
	level0, ok := levels["0"].(map[string]interface{})
	if !ok {
		return false
	}

	statsUplink, _ := level0["statsUserUplink"].(bool)
	statsDownlink, _ := level0["statsUserDownlink"].(bool)

	return statsUplink && statsDownlink
}

func (h *ManageHandler) getTemplatePolicy() map[string]interface{} {
	return map[string]interface{}{
		"levels": map[string]interface{}{
			"0": map[string]interface{}{
				"handshake":         float64(5),
				"connIdle":          float64(300),
				"uplinkOnly":        float64(2),
				"downlinkOnly":      float64(2),
				"statsUserUplink":   true,
				"statsUserDownlink": true,
			},
		},
		"system": map[string]interface{}{
			"statsInboundUplink":    true,
			"statsInboundDownlink":  true,
			"statsOutboundUplink":   true,
			"statsOutboundDownlink": true,
		},
	}
}

func (h *ManageHandler) hasAPIInbound(config map[string]interface{}) bool {
	inbounds, ok := config["inbounds"].([]interface{})
	if !ok {
		return false
	}
	for _, ib := range inbounds {
		if inbound, ok := ib.(map[string]interface{}); ok {
			if tag, _ := inbound["tag"].(string); tag == "api" {
				return true
			}
		}
	}
	return false
}

func (h *ManageHandler) addAPIInbound(config map[string]interface{}) {
	apiInbound := map[string]interface{}{
		"tag":      "api",
		"port":     float64(46736),
		"listen":   constants.LocalhostIP,
		"protocol": "dokodemo-door",
		"settings": map[string]interface{}{
			"address": constants.LocalhostIP,
		},
	}

	inbounds, ok := config["inbounds"].([]interface{})
	if !ok {
		inbounds = []interface{}{}
	}
	config["inbounds"] = append([]interface{}{apiInbound}, inbounds...)
}

func (h *ManageHandler) hasAPIRoutingRule(config map[string]interface{}) bool {
	routing, ok := config["routing"].(map[string]interface{})
	if !ok {
		return false
	}
	rules, ok := routing["rules"].([]interface{})
	if !ok {
		return false
	}
	for _, r := range rules {
		if rule, ok := r.(map[string]interface{}); ok {
			if outboundTag, _ := rule["outboundTag"].(string); outboundTag == "api" {
				return true
			}
		}
	}
	return false
}

func (h *ManageHandler) addAPIRoutingRule(config map[string]interface{}) {
	apiRule := map[string]interface{}{
		"type":        "field",
		"inboundTag":  []interface{}{"api"},
		"outboundTag": "api",
	}

	routing, ok := config["routing"].(map[string]interface{})
	if !ok {
		routing = map[string]interface{}{
			"domainStrategy": "IPIfNonMatch",
			"rules":          []interface{}{},
		}
		config["routing"] = routing
	}

	rules, ok := routing["rules"].([]interface{})
	if !ok {
		rules = []interface{}{}
	}
	routing["rules"] = append([]interface{}{apiRule}, rules...)
}

func (h *ManageHandler) hasRequiredOutbounds(config map[string]interface{}) bool {
	outbounds, ok := config["outbounds"].([]interface{})
	if !ok {
		return false
	}
	hasDirect, hasBlock := false, false
	for _, ob := range outbounds {
		if outbound, ok := ob.(map[string]interface{}); ok {
			switch tag, _ := outbound["tag"].(string); tag {
			case "direct":
				hasDirect = true
			case "block":
				hasBlock = true
			}
		}
	}
	return hasDirect && hasBlock
}

func (h *ManageHandler) addRequiredOutbounds(config map[string]interface{}) {
	outbounds, ok := config["outbounds"].([]interface{})
	if !ok {
		outbounds = []interface{}{}
	}

	tags := map[string]bool{}
	for _, ob := range outbounds {
		if outbound, ok := ob.(map[string]interface{}); ok {
			if tag, _ := outbound["tag"].(string); tag != "" {
				tags[tag] = true
			}
		}
	}

	if !tags["direct"] {
		outbounds = append(outbounds, map[string]interface{}{
			"tag":      "direct",
			"protocol": "freedom",
		})
	}
	if !tags["block"] {
		outbounds = append(outbounds, map[string]interface{}{
			"tag":      "block",
			"protocol": "blackhole",
		})
	}
	config["outbounds"] = outbounds
}

func (h *ManageHandler) ensureRoutingRules(config map[string]interface{}) bool {
	routing, ok := config["routing"].(map[string]interface{})
	if !ok {
		routing = map[string]interface{}{
			"domainStrategy": "IPIfNonMatch",
			"rules":          []interface{}{},
		}
		config["routing"] = routing
	}

	rules, ok := routing["rules"].([]interface{})
	if !ok {
		rules = []interface{}{}
	}

	existingMarktags := map[string]bool{}
	for _, r := range rules {
		if rule, ok := r.(map[string]interface{}); ok {
			if mt, _ := rule["marktag"].(string); mt != "" {
				existingMarktags[mt] = true
			}
		}
	}

	added := false
	requiredRules := []map[string]interface{}{
		{"type": "field", "protocol": []interface{}{"bittorrent"}, "marktag": "ban_bt", "outboundTag": "block"},
		{"type": "field", "ip": []interface{}{"geoip:cn"}, "marktag": "ban_geoip_cn", "outboundTag": "block"},
		{"type": "field", "ip": []interface{}{"geoip:private"}, "outboundTag": "block"},
	}

	for _, req := range requiredRules {
		marktag, _ := req["marktag"].(string)
		if marktag != "" && existingMarktags[marktag] {
			continue
		}
		// geoip:private 没有 marktag，按 ip 内容判断
		if marktag == "" {
			found := false
			for _, r := range rules {
				if rule, ok := r.(map[string]interface{}); ok {
					if ips, ok := rule["ip"].([]interface{}); ok {
						for _, ip := range ips {
							if ip == "geoip:private" {
								found = true
								break
							}
						}
					}
				}
				if found {
					break
				}
			}
			if found {
				continue
			}
		}
		rules = append(rules, req)
		added = true
	}

	if added {
		routing["rules"] = rules
	}
	return added
}

// removeDeprecatedRoutingRules 一次性清理已废弃的路由规则(按 marktag 白名单匹配)。
// 历史版本 ensureRoutingRules 注入过 fix_openai 等规则,新版本不再需要,但残留在老 agent 的 config.json 里 —
// 启动期顺手清掉,避免长期跟主控 snapshot 不一致触发"配置漂移"提示。
// 返回 true 表示有改动,调用方据此触发 marshal+writeFile(沿用 EnsureXrayConfig 已有写回路径)。
// 幂等:第二次启动 rules 里已无废弃 marktag → 返回 false,no-op。
func (h *ManageHandler) removeDeprecatedRoutingRules(config map[string]interface{}) bool {
	deprecated := map[string]bool{
		"fix_openai": true, // 8a9f8c9 移除,geosite:openai → direct
	}
	routing, _ := config["routing"].(map[string]interface{})
	if routing == nil {
		return false
	}
	rules, _ := routing["rules"].([]interface{})
	if len(rules) == 0 {
		return false
	}
	kept := make([]interface{}, 0, len(rules))
	removed := 0
	for _, r := range rules {
		rule, _ := r.(map[string]interface{})
		if rule != nil {
			if tag, _ := rule["marktag"].(string); deprecated[tag] {
				removed++
				continue
			}
		}
		kept = append(kept, r)
	}
	if removed == 0 {
		return false
	}
	routing["rules"] = kept
	log.Printf("[EnsureXrayConfig] removeDeprecatedRoutingRules: removed %d rule(s) (deprecated marktag)", removed)
	return true
}

// ================== 证书部署 ==================

// CertDeployRequest 表示主控端下发的证书部署请求。
type CertDeployRequest struct {
	Domain   string `json:"domain"`
	CertPEM  string `json:"cert_pem"`
	KeyPEM   string `json:"key_pem"`
	CertPath string `json:"cert_path"`
	KeyPath  string `json:"key_path"`
	Reload   string `json:"reload"` // nginx, xray, both, none
}

// 处理 POST /api/child/cert/deploy。
func (h *ManageHandler) HandleCertDeploy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	var req CertDeployRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.CertPEM == "" || req.KeyPEM == "" || req.CertPath == "" || req.KeyPath == "" {
		writeError(w, http.StatusBadRequest, "cert_pem, key_pem, cert_path, key_path are required")
		return
	}

	if err := deployCertFiles(req.CertPEM, req.KeyPEM, req.CertPath, req.KeyPath, req.Reload); err != nil {
		log.Printf("[CertDeploy] Failed to deploy cert for %s: %v", req.Domain, err)
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("deploy failed: %v", err))
		return
	}

	log.Printf("[CertDeploy] Successfully deployed cert for %s to %s", req.Domain, req.CertPath)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("certificate for %s deployed", req.Domain),
	})
}

func deployCertFiles(certPEM, keyPEM, certPath, keyPath, reloadTarget string) error {
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
		return reloadNginx()
	case "xray":
		return xrayctl.RestartXray("auto", "")
	case "both":
		if err := reloadNginx(); err != nil {
			return err
		}
		return xrayctl.RestartXray("auto", "")
	}
	return nil
}

func deployNginxSSLConfig(domain string) {
	domain = strings.TrimSpace(domain)
	if domain == "" {
		return
	}

	confDir := constants.NginxConfigDirPaths[4]
	if _, err := os.Stat(confDir); err != nil {
		confDir = constants.NginxConfigDirPaths[0]
	}

	certDir := filepath.Join(confDir, "cert")
	os.MkdirAll(certDir, 0755)

	serverBlock := fmt.Sprintf(`server {
    listen 443 ssl default_server;
    listen [::]:443 ssl;
    http2 on;
    server_name %s;
    ssl_certificate  cert/%s.pem;
    ssl_certificate_key cert/%s.key;
    ssl_session_timeout 5m;
    ssl_ciphers ECDHE-RSA-AES128-GCM-SHA256:ECDHE:ECDH:AES:HIGH:!NULL:!aNULL:!MD5:!ADH:!RC4;
    ssl_protocols TLSv1 TLSv1.1 TLSv1.2;

    location / {
        root /usr/local/nginx/html;
        index index.html;
    }
}
`, domain, domain, domain)

	// 写入 conf.d，或通过 include 挂载
	confDDir := filepath.Join(confDir, "conf.d")
	os.MkdirAll(confDDir, 0755)
	sslConfPath := filepath.Join(confDDir, "ssl.conf")

	if err := os.WriteFile(sslConfPath, []byte(serverBlock), 0644); err != nil {
		log.Printf("[Manage] Failed to write nginx SSL config: %v", err)
		return
	}

	// 确保主 nginx.conf 包含 conf.d/*.conf
	mainConf := filepath.Join(confDir, "nginx.conf")
	content, err := os.ReadFile(mainConf)
	if err != nil {
		log.Printf("[Manage] Failed to read nginx.conf: %v", err)
		return
	}

	includeDirective := "include conf.d/*.conf;"
	if !strings.Contains(string(content), includeDirective) {
		// 在 http 块最后一个右括号前插入 include
		text := string(content)
		lastBrace := strings.LastIndex(text, "}")
		if lastBrace > 0 {
			text = text[:lastBrace] + "    " + includeDirective + "\n" + text[lastBrace:]
			if err := os.WriteFile(mainConf, []byte(text), 0644); err != nil {
				log.Printf("[Manage] Failed to update nginx.conf with include: %v", err)
				return
			}
		}
	}

	log.Printf("[Manage] Nginx SSL config deployed for domain %s at %s", domain, sslConfPath)
}

// HandleNginxSetupSSL 处理 POST /api/child/nginx/setup-ssl。
// 部署 nginx.conf 和 servers/{domain}.conf。
func (h *ManageHandler) HandleNginxSetupSSL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	var req struct {
		Domain       string `json:"domain"`
		NginxConfig  string `json:"nginx_config"`
		DomainConfig string `json:"domain_config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Domain == "" {
		writeError(w, http.StatusBadRequest, "domain is required")
		return
	}

	domain := strings.ToLower(strings.TrimSpace(req.Domain))

	// confDir 选择:优先用 nginx -V 拿编译路径(权威),不用就 fallback stat(老逻辑兼容)。
	// 老逻辑的 bug:install-nginx.sh 异步装时,stat 容易在 5s deploy 窗口里返回"不存在" →
	// 写到 /etc/nginx → nginx 装完后实际 conf-path 是 /usr/local/nginx/nginx.conf → 读不到。
	confDir := detectNginxConfDirFromBinary()
	confDirAuthoritative := confDir != "" // detect 出的 conf-path 100% 可信,跟 nginx 跑时一致
	if confDir == "" {
		confDir = constants.NginxPrimaryPrefixDir
		if _, err := os.Stat(confDir); err != nil {
			confDir = constants.NginxConfigDirPaths[0]
		}
	}
	log.Printf("[Manage] setup-ssl confDir=%s authoritative=%v (domain=%s)", confDir, confDirAuthoritative, domain)

	// 确保证书和 servers 目录存在
	os.MkdirAll(filepath.Join(confDir, "cert"), 0755)
	os.MkdirAll(filepath.Join(confDir, "servers"), 0755)

	if req.NginxConfig != "" {
		// 下发主 nginx.conf
		mainConf := filepath.Join(confDir, "nginx.conf")
		if content, err := os.ReadFile(mainConf); err == nil {
			os.WriteFile(mainConf+".bak."+time.Now().Format("20060102150405"), content, 0644)
		}
		if err := os.WriteFile(mainConf, []byte(req.NginxConfig), 0644); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to write nginx.conf: %v", err))
			return
		}
		log.Printf("[Manage] nginx.conf deployed at %s", mainConf)
	}

	if req.DomainConfig != "" {
		// 下发域名 server 配置到 servers/{domain}.conf
		domainConfPath := filepath.Join(confDir, "servers", domain+".conf")
		if err := os.WriteFile(domainConfPath, []byte(req.DomainConfig), 0644); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to write domain config: %v", err))
			return
		}
		log.Printf("[Manage] Domain config deployed at %s", domainConfPath)
	} else {
		// 兜底：沿用旧逻辑
		deployNginxSSLConfig(domain)
	}

	// 重载 nginx 使配置生效。reload 失败 + confDir 是猜的(不 authoritative) → 大概率写错位置,
	// 必须让主控知道(返 500),避免老 bug:写到 /etc/nginx 但 nginx 跑 /usr/local/nginx,reload OK 200 假成功。
	// reload 失败 + confDir 权威 → 配置位置 OK,失败大概率是语法 / 权限,仍 200(用户/主控可后续手动 reload)。
	if err := reloadNginx(); err != nil {
		log.Printf("[Manage] Nginx reload after setup-ssl failed (confDir=%s authoritative=%v): %v", confDir, confDirAuthoritative, err)
		if !confDirAuthoritative {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("nginx reload failed and confDir was guessed (%s); please ensure nginx is installed then retry: %v", confDir, err))
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("SSL config deployed for %s", domain),
	})
}

// HandleNginxServersList 列出本机 nginx servers/ 目录下所有 *.conf 域名文件,
// 用于前端在添加 vless+wss 入站前检测目标域名是否已被 reality / 旧 wss 占用。
// 扫描目录:detectNginxConfDirFromBinary 拿到的权威 confDir 的 servers/ 子目录 +
// NginxSSLServerDirPaths 列表(冗余兜底 — 历史装机可能落在不同前缀)。dedup by domain。
func (h *ManageHandler) HandleNginxServersList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	// 扫描目录集合 = 权威 confDir/servers + 已知 fallback 路径,dedup
	dirs := make([]string, 0, len(constants.NginxSSLServerDirPaths)+1)
	if confDir := detectNginxConfDirFromBinary(); confDir != "" {
		dirs = append(dirs, filepath.Join(confDir, "servers"))
	}
	dirs = append(dirs, constants.NginxSSLServerDirPaths...)

	type domainEntry struct {
		Domain  string `json:"domain"`
		Path    string `json:"path"`
		ModTime string `json:"mod_time"`
		Size    int64  `json:"size"`
	}
	seen := make(map[string]bool)
	domains := []domainEntry{}
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".conf") {
				continue
			}
			domain := strings.ToLower(strings.TrimSuffix(e.Name(), ".conf"))
			if seen[domain] {
				continue
			}
			seen[domain] = true
			fullPath := filepath.Join(dir, e.Name())
			info, ierr := e.Info()
			if ierr != nil {
				domains = append(domains, domainEntry{Domain: domain, Path: fullPath})
				continue
			}
			domains = append(domains, domainEntry{
				Domain:  domain,
				Path:    fullPath,
				ModTime: info.ModTime().UTC().Format(time.RFC3339),
				Size:    info.Size(),
			})
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"domains": domains,
	})
}

func (h *ManageHandler) HandleValidateSite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	var req struct {
		SiteType  string `json:"site_type"`
		SiteValue string `json:"site_value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.SiteValue == "" {
		writeError(w, http.StatusBadRequest, "site_value is required")
		return
	}

	switch req.SiteType {
	case "static":
		indexPath := filepath.Join(req.SiteValue, "index.html")
		if _, err := os.Stat(indexPath); err != nil {
			writeJSON(w, http.StatusOK, map[string]any{
				"success": false,
				"message": fmt.Sprintf("index.html not found at %s", req.SiteValue),
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"success": true,
			"message": "index.html exists",
		})
	case "proxy":
		ctx, cancel := context.WithTimeout(r.Context(), 6*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--connect-timeout", "5", req.SiteValue)
		out, err := cmd.Output()
		if err != nil {
			writeJSON(w, http.StatusOK, map[string]any{
				"success": false,
				"message": fmt.Sprintf("connection failed: %v", err),
			})
			return
		}
		code := strings.TrimSpace(string(out))
		writeJSON(w, http.StatusOK, map[string]any{
			"success": true,
			"message": fmt.Sprintf("HTTP %s", code),
		})
	default:
		writeError(w, http.StatusBadRequest, "site_type must be 'static' or 'proxy'")
	}
}

// detectNginxConfDirFromBinary 用 `nginx -V` 拿到编译时的 conf-path,返回它的父目录。
//
// 解决的 race:`stealSelfDeployer` 在 agent 上线后 5s 触发 setup-ssl,但 install-nginx.sh 是异步跑,
// 5s 内 `/usr/local/nginx/` 可能还没建好 → 老逻辑 `os.Stat` 失败 → fallback 到 `/etc/nginx/` → 配置
// 写到那里,但 nginx 装完后实际跑的是 `/usr/local/nginx/nginx.conf`(编译路径) → 根本读不到 → 用户
// 永远看不到 ssl 配置生效。
//
// 用 nginx -V 才能拿到权威的 conf-path,不再依赖目录是否存在的快照式判断。
// 找不到二进制 / 解析失败 → 返回空串,调用方走原 stat fallback(老 agent 兼容)。
func detectNginxConfDirFromBinary() string {
	var bin string
	for _, p := range constants.NginxBinarySearchPaths {
		if path, err := exec.LookPath(p); err == nil {
			bin = path
			break
		}
	}
	if bin == "" {
		return ""
	}
	// nginx -V 把信息打到 stderr。CombinedOutput 一并捕获,简单粗暴
	out, err := exec.Command(bin, "-V").CombinedOutput()
	if err != nil {
		return ""
	}
	// 输出形如 `--conf-path=/usr/local/nginx/nginx.conf`,取出 dir 部分
	re := regexp.MustCompile(`--conf-path=(\S+)/nginx\.conf`)
	m := re.FindStringSubmatch(string(out))
	if len(m) >= 2 {
		return m[1]
	}
	return ""
}

func reloadNginx() error {
	for _, bin := range constants.NginxBinarySearchPaths {
		if path, err := exec.LookPath(bin); err == nil {
			return runCommand(path, "-s", "reload")
		}
	}
	return runCommand("systemctl", "reload", "nginx")
}

func runCommand(name string, args ...string) error {
	if output, err := exec.Command(name, args...).CombinedOutput(); err != nil {
		return fmt.Errorf("%s: %s: %w", name, string(output), err)
	}
	return nil
}

func (h *ManageHandler) HandleClearStreamPort(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	var req struct {
		Port int `json:"port"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Port <= 0 {
		writeError(w, http.StatusBadRequest, "valid port required")
		return
	}

	streamDir := filepath.Join(constants.NginxPrimaryPrefixDir, "stream_servers")
	if _, err := os.Stat(streamDir); err != nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"removed": 0,
			"message": "stream_servers directory not found, nothing to clean",
		})
		return
	}

	entries, err := os.ReadDir(streamDir)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to read stream_servers: %v", err))
		return
	}

	listenPattern := fmt.Sprintf("listen %d", req.Port)
	var removed []string

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		filePath := filepath.Join(streamDir, entry.Name())
		content, err := os.ReadFile(filePath)
		if err != nil {
			continue
		}
		if strings.Contains(string(content), listenPattern) {
			if err := os.Remove(filePath); err == nil {
				removed = append(removed, entry.Name())
				log.Printf("[Manage] Removed stream config %s (listening on port %d)", entry.Name(), req.Port)
			}
		}
	}

	if len(removed) > 0 {
		// 从 nginx.conf 中移除 stream 块
		nginxConfPath := filepath.Join(constants.NginxPrimaryPrefixDir, "nginx.conf")
		if confData, err := os.ReadFile(nginxConfPath); err == nil {
			conf := string(confData)
			streamBlock := "\nstream {\n    include stream_servers/*.conf;\n}\n"
			if cleaned := strings.Replace(conf, streamBlock, "\n", 1); cleaned != conf {
				os.WriteFile(nginxConfPath, []byte(cleaned), 0644)
				log.Printf("[Manage] Removed stream block from nginx.conf")
			}
		}

		if err := reloadNginx(); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("removed %d files but nginx reload failed: %v", len(removed), err))
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"removed": len(removed),
		"files":   removed,
		"message": fmt.Sprintf("removed %d stream config(s) listening on port %d", len(removed), req.Port),
	})
}

// DeployDefaultXrayConfigFile 仅写入内置默认配置文件，不启动 xray。
func (h *ManageHandler) DeployDefaultXrayConfigFile() {
	configPath := constants.DefaultXrayConfigPaths[0]
	if _, err := os.Stat(configPath); err == nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
		log.Printf("[Manage] Failed to create xray config dir: %v", err)
		return
	}
	if err := os.WriteFile(configPath, defaultXrayConfig, 0644); err != nil {
		log.Printf("[Manage] Failed to write default xray config: %v", err)
		return
	}
	log.Printf("[Manage] Deployed default xray config to %s", configPath)
}

// DeployDefaultXrayConfig 在缺失配置时写入内置默认配置并启动 xray。
func (h *ManageHandler) DeployDefaultXrayConfig() {
	configPath := constants.DefaultXrayConfigPaths[0]
	if _, err := os.Stat(configPath); err == nil {
		// 配置已存在，执行 EnsureXrayConfig 补齐缺失段
		result := h.EnsureXrayConfig()
		if result.Modified {
			log.Printf("[Manage] Xray config updated after install: added %v", result.AddedSections)
			h.RestartXray()
		}
		return
	}

	if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
		log.Printf("[Manage] Failed to create xray config dir: %v", err)
		return
	}
	if err := os.WriteFile(configPath, defaultXrayConfig, 0644); err != nil {
		log.Printf("[Manage] Failed to write default xray config: %v", err)
		return
	}
	log.Printf("[Manage] Deployed default xray config to %s", configPath)
	h.RestartXray()
}

// ================== SSE 流式安装/卸载 ==================

func sseStreamCmd(w http.ResponseWriter, r *http.Request, cmd *exec.Cmd, completeMsg string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		sseEvent(w, flusher, map[string]string{"type": "error", "message": err.Error()})
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		sseEvent(w, flusher, map[string]string{"type": "error", "message": err.Error()})
		return
	}

	if err := cmd.Start(); err != nil {
		sseEvent(w, flusher, map[string]string{"type": "error", "message": err.Error()})
		return
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(2)

	scanStream := func(rc io.ReadCloser) {
		defer wg.Done()
		scanner := bufio.NewScanner(rc)
		scanner.Buffer(make([]byte, 256*1024), 256*1024)
		for scanner.Scan() {
			select {
			case <-r.Context().Done():
				return
			default:
			}
			mu.Lock()
			sseEvent(w, flusher, map[string]string{"type": "output", "data": scanner.Text()})
			mu.Unlock()
		}
	}

	go scanStream(stdout)
	go scanStream(stderr)

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		wg.Wait()
		if err != nil {
			sseEvent(w, flusher, map[string]string{"type": "error", "message": err.Error()})
		} else {
			sseEvent(w, flusher, map[string]interface{}{"type": "complete", "success": true, "message": completeMsg})
		}
	case <-r.Context().Done():
		cmd.Process.Kill()
		sseEvent(w, flusher, map[string]string{"type": "error", "message": "request cancelled"})
	}
}

func sseEvent(w http.ResponseWriter, flusher http.Flusher, data interface{}) {
	b, _ := json.Marshal(data)
	fmt.Fprintf(w, "data: %s\n\n", b)
	flusher.Flush()
}

func (h *ManageHandler) HandleXrayInstallStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	log.Printf("[Manage] Starting Xray install (stream)...")
	cmd := exec.CommandContext(r.Context(), "bash", "-c",
		`bash -c "$(curl -L https://github.com/XTLS/Xray-install/raw/main/install-release.sh)" @ install`)
	cmd.Env = os.Environ()
	sseStreamCmd(w, r, cmd, "Xray installed successfully")

	// 安装完成后下发默认配置
	h.DeployDefaultXrayConfig()
}

func (h *ManageHandler) HandleXrayRemoveStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	log.Printf("[Manage] Starting Xray remove (stream)...")
	cmd := exec.CommandContext(r.Context(), "bash", "-c",
		`bash -c "$(curl -L https://github.com/XTLS/Xray-install/raw/main/install-release.sh)" @ remove`)
	cmd.Env = os.Environ()
	sseStreamCmd(w, r, cmd, "Xray removed successfully")
}

func (h *ManageHandler) HandleNginxInstallStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	if nginxInstalling.Load() {
		writeError(w, http.StatusConflict, "Nginx installation already in progress")
		return
	}
	nginxInstalling.Store(true)
	defer nginxInstalling.Store(false)

	domain := r.URL.Query().Get("domain")

	log.Printf("[Manage] Starting Nginx install (stream)...")
	cmd := exec.CommandContext(r.Context(), "bash", "-c",
		`set -e; SCRIPT=$(mktemp); curl -fsSL https://raw.githubusercontent.com/iluobei/miaomiaowuX/main/install-nginx.sh -o "$SCRIPT"; bash "$SCRIPT"; rm -f "$SCRIPT"`)
	cmd.Env = os.Environ()
	sseStreamCmd(w, r, cmd, "Nginx installed successfully")

	if domain != "" {
		deployNginxSSLConfig(domain)
	}
}

func (h *ManageHandler) HandleNginxRemoveStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	log.Printf("[Manage] Starting Nginx remove (stream)...")
	cmd := exec.CommandContext(r.Context(), "bash", "-c",
		`curl -fsSL https://raw.githubusercontent.com/iluobei/miaomiaowuX/main/uninstall-nginx.sh | bash -s -- -y`)
	cmd.Env = os.Environ()
	sseStreamCmd(w, r, cmd, "Nginx removed successfully")
}

func (h *ManageHandler) HandleAgentUpgradeStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	log.Printf("[Manage] Starting Agent upgrade (stream)...")
	// 升级流程:
	//   1. 脚本里只做"下载 + 校验 + 替换二进制",不再嵌入 `systemctl restart`
	//      (旧实现把 systemctl restart 放在 nohup bash 里,bash 在 mmw-agent.service 的 cgroup,
	//       systemd 杀 cgroup 时把 bash 也杀掉,/tmp 文件不会清理,且偶发不重启 — 见 port/xray_mode 切换
	//       同款问题的修复)
	//   2. 脚本退出 → sseStreamCmd 收到 complete → goroutine 里 os.Exit(0)
	//   3. systemd Restart=always 拉起新二进制(/usr/local/bin/mmw-agent 已经被脚本里的 cp 覆盖)
	script := `
set -e
echo "=========================================="
echo "  MMW-Agent Upgrade"
echo "=========================================="

ARCH=$(uname -m)
case $ARCH in
    x86_64)  ARCH_NAME="amd64" ;;
    aarch64|arm64) ARCH_NAME="arm64" ;;
    *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

# 镜像链 — 顺序尝试,任一成功即停。
# 必要性:GitHub Release binary 实际重定向到 objects.githubusercontent.com,该域名只有 A 记录(无 AAAA),
# 纯 v6 机器(如澳门 Debee mo-d.2ha.me)直连 github 会 "network is unreachable" → 升级永远失败。
# ghproxy / gh-proxy 提供 v4+v6 双栈反代,放在前面让 v6-only 机器优先命中。
MIRRORS=(
    "https://mirror.ghproxy.com/https://github.com/iluobei/mmw-agent/releases/latest/download/mmw-agent-linux-${ARCH_NAME}"
    "https://gh-proxy.com/https://github.com/iluobei/mmw-agent/releases/latest/download/mmw-agent-linux-${ARCH_NAME}"
    "https://github.com/iluobei/mmw-agent/releases/latest/download/mmw-agent-linux-${ARCH_NAME}"
)
# 优先 curl,没有就用 wget;两者都没就按发行版包管理器装一个 — 跟 install.sh 同款逻辑
if ! command -v curl >/dev/null 2>&1 && ! command -v wget >/dev/null 2>&1; then
    echo "未检测到 curl/wget,尝试自动安装 curl..."
    if command -v apt-get >/dev/null 2>&1; then
        apt-get update -qq >/dev/null 2>&1 || true
        DEBIAN_FRONTEND=noninteractive apt-get install -y curl
    elif command -v dnf >/dev/null 2>&1; then
        dnf install -y curl
    elif command -v yum >/dev/null 2>&1; then
        yum install -y curl
    elif command -v apk >/dev/null 2>&1; then
        apk add --no-cache curl
    elif command -v pacman >/dev/null 2>&1; then
        pacman -Sy --noconfirm curl
    elif command -v zypper >/dev/null 2>&1; then
        zypper -n install curl
    else
        echo "ERROR: 无法识别系统包管理器,请手动安装 curl 或 wget" >&2
        exit 1
    fi
fi
# --connect-timeout TCP 握手最长 10s;--max-time 整个传输上限 180s — 镜像偶发卡死时,不加 max-time 会无声 hang。
download_ok=0
for url in "${MIRRORS[@]}"; do
    echo "Downloading from $url ..."
    if command -v curl >/dev/null 2>&1; then
        if curl -fsSL --connect-timeout 10 --max-time 180 -o /tmp/mmw-agent-new "$url"; then
            download_ok=1
            break
        fi
    else
        if wget -q --connect-timeout=10 --read-timeout=180 -O /tmp/mmw-agent-new "$url"; then
            download_ok=1
            break
        fi
    fi
    echo "  → 该镜像失败,尝试下一个..."
done
if [ "$download_ok" != "1" ]; then
    echo "ERROR: 所有镜像均下载失败(GitHub + ghproxy + gh-proxy 全部不可达)" >&2
    exit 1
fi

chmod +x /tmp/mmw-agent-new
echo "Download complete, binary size: $(du -h /tmp/mmw-agent-new | cut -f1)"

# 替换二进制(systemd Restart=always 会在 agent 退出后拉起新版本)。
# 直接 cp 到 /usr/local/bin/mmw-agent 会触发 "Text file busy",因为正在运行的 mmw-agent 进程占着该 inode。
# 改成"先 cp 到旁路文件,再原子 mv 覆盖" — Linux rename(2) 不影响正在执行的进程映射的旧 inode,
# 新 inode 接管该路径,旧 inode 直到进程退出才释放。
cp /tmp/mmw-agent-new /usr/local/bin/mmw-agent.new
chmod +x /usr/local/bin/mmw-agent.new
mv -f /usr/local/bin/mmw-agent.new /usr/local/bin/mmw-agent
rm -f /tmp/mmw-agent-new
echo "Binary replaced; agent will exit and systemd will restart with new version."
`
	// 整个升级流程兜底超时 5 分钟 — 包括 GitHub 下载、二进制写入、SSE 流。
	// 之前没设上限,sseStreamCmd 卡在 cmd.Wait + SSE 写之间能挂 2+ 天不释放 handler 协程
	// (us-a.2ha.me 实例:"Starting Agent upgrade" 日志后再无任何后续日志,goroutine 永久泄漏)。
	// CommandContext 收到 cancel 会 SIGKILL bash,sseStreamCmd 在 r.Context().Done() 分支返回,
	// streamDone 写 false → 日志 "script failed" → handler 干净退出。
	upgradeCtx, upgradeCancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer upgradeCancel()
	cmdReq := r.Clone(upgradeCtx)
	cmd := exec.CommandContext(upgradeCtx, "bash", "-c", script)
	cmd.Env = os.Environ()
	// sseStreamCmd 内部 Wait() 完命令才返回,所以 script 成功跑完(包括 cp 替换二进制)后这里才走到下面
	// 用 channel 让 sseStreamCmd 阻塞期间不退出,完成后 os.Exit。
	streamDone := make(chan bool, 1)
	go func() {
		sseStreamCmd(w, cmdReq, cmd, "Agent upgraded, restarting...")
		streamDone <- (cmd.ProcessState != nil && cmd.ProcessState.Success())
	}()
	var success bool
	select {
	case success = <-streamDone:
	case <-time.After(6 * time.Minute):
		// 超过 sseStreamCmd 自身应该在 upgradeCtx (5min) 后退出的时间,仍未结束 → 强制兜底
		log.Printf("[Manage] Agent upgrade hard timeout (>6min); abandoning goroutine and giving up")
		upgradeCancel()
		return
	}
	if !success {
		log.Printf("[Manage] Agent upgrade script failed; not exiting")
		return
	}
	// 升级完成后:不调 os.Exit,而是 syscall.Exec 把当前进程"原地替换"成新 binary。
	// 优势 — 跟 supervisor 解耦:
	//   - PID 不变 → systemd / supervise-daemon 看不到子进程退出 → 不会触发 restart → 没有
	//     "supervisor respawn 抢 socket"的 race(OpenRC supervise-daemon 上特别明显:
	//      rc-service restart 自己都能因为 respawn_delay race 创出双开,何况升级路径)
	//   - 不需要 self-respawn fork → 没有"自己 fork 一个 + supervisor 又来一个"双开
	//   - 唯一的副作用:embedded xray 在 exec 那一刹那一起死,新 binary 启动时重读 config 重建。
	//     已经是升级的预期行为。
	//
	// 500ms 让 SSE complete 真的送到客户端。失败兜底 os.Exit。
	go func() {
		time.Sleep(500 * time.Millisecond)
		binPath, err := os.Executable()
		if err != nil || binPath == "" {
			binPath = "/usr/local/bin/mmw-agent"
		}
		log.Printf("[Manage] Self-exec %s after upgrade (PID unchanged, no supervisor restart needed)", binPath)
		if execErr := syscall.Exec(binPath, os.Args, os.Environ()); execErr != nil {
			// 极端情况(binary 被删 / 权限错误)走老退出路径让 supervisor 拉起
			log.Printf("[Manage] Self-exec failed: %v; falling back to exit and let supervisor restart", execErr)
			os.Exit(0)
		}
		// 正常情况下 syscall.Exec 永远不返回。代码到这里说明返回 nil 又没 exec 成功(几乎不可能),兜底退出。
		os.Exit(0)
	}()
}

// someoneWillRestartAgent 判断当前 agent 退出后是否有外部 supervisor 会自动拉起新进程。
// 任一命中即返回 true,scheduleSelfRespawn 应跳过 — 否则会跟 supervisor 双开抢端口,
// 出现"两个 mmw-agent 进程同时启动 → 一个 bind 失败循环退出 → 持续 flap"的现象。
//
// 识别顺序:
//  1. systemd:/run/systemd/system 存在 + systemctl is-active mmw-agent
//  2. OpenRC / runit / s6:agent 的父进程是已知 supervisor(supervise-daemon / runsv / s6-supervise)
func someoneWillRestartAgent() bool {
	// 1) systemd
	if _, err := os.Stat("/run/systemd/system"); err == nil {
		if out, err := exec.Command("systemctl", "is-active", "mmw-agent").Output(); err == nil {
			if strings.TrimSpace(string(out)) == "active" {
				return true
			}
		}
	}
	// 2) 父进程是已知 supervisor — 兜住 LXC + OpenRC supervise-daemon、runit、s6 等场景
	if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", os.Getppid())); err == nil {
		name := strings.TrimSpace(string(data))
		switch name {
		case "supervise-daemon", "runsv", "s6-supervise":
			return true
		}
	}
	return false
}

// scheduleSelfRespawn 在当前进程退出前 fork 一个完全脱离的子进程,
// 子进程 sleep 2s 后 exec 新二进制(此时旧进程已退出,端口可用)。
// Setsid 让子进程脱离当前会话/进程组,父进程退出后不会被收走。
func scheduleSelfRespawn() {
	if len(os.Args) == 0 {
		log.Printf("[Manage] self-respawn: os.Args empty, skip")
		return
	}
	// 用 /bin/sh 包一层 sleep + exec,确保旧进程退出释放端口后再启动新进程。
	// 单引号包路径 / 参数 — 防 shell 元字符;若参数本身含单引号,exec 会失败但 systemd 路径也会同样失败,极少见。
	quoted := make([]string, len(os.Args))
	for i, a := range os.Args {
		quoted[i] = "'" + strings.ReplaceAll(a, "'", `'\''`) + "'"
	}
	script := "sleep 2; exec " + strings.Join(quoted, " ")
	cmd := exec.Command("/bin/sh", "-c", script)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		log.Printf("[Manage] self-respawn: start failed: %v", err)
		return
	}
	log.Printf("[Manage] self-respawn: detached pid=%d will exec new binary in 2s", cmd.Process.Pid)
}

func (h *ManageHandler) HandleAgentUninstallStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	log.Printf("[Manage] Starting Agent uninstall (stream)...")
	script := `
set -e
echo "=========================================="
echo "  MMW-Agent Uninstall"
echo "=========================================="

echo "Scheduling delayed uninstall..."
nohup bash -c 'sleep 2 && systemctl stop mmw-agent && systemctl disable mmw-agent && rm -f /usr/local/bin/mmw-agent && rm -f /etc/systemd/system/mmw-agent.service && systemctl daemon-reload && rm -rf /etc/mmw-agent /var/lib/mmw-agent && echo "Agent uninstalled"' >/dev/null 2>&1 &
echo "Agent will be uninstalled in a few seconds."
`
	cmd := exec.CommandContext(r.Context(), "bash", "-c", script)
	cmd.Env = os.Environ()
	sseStreamCmd(w, r, cmd, "Agent uninstall scheduled")
}

// HandleLimiter 处理 POST /api/child/limiter，用于直接配置嵌入式 Xray 的限速。
func (h *ManageHandler) HandleLimiter(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	if h.embeddedXray == nil {
		writeError(w, http.StatusBadRequest, "Not in embedded mode")
		return
	}

	var req struct {
		InboundTag string `json:"inbound_tag"`
		NodeLimit  uint64 `json:"node_limit"`
		Users      []struct {
			UID         int    `json:"uid"`
			Email       string `json:"email"`
			SpeedLimit  uint64 `json:"speed_limit"`
			DeviceLimit int    `json:"device_limit"`
		} `json:"users"`
		AutoSpeedRules []embedded.AutoSpeedLimitRule `json:"auto_speed_rules,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	users := make([]limiter.UserInfo, len(req.Users))
	for i, u := range req.Users {
		users[i] = limiter.UserInfo{
			UID:         u.UID,
			Email:       u.Email,
			SpeedLimit:  u.SpeedLimit,
			DeviceLimit: u.DeviceLimit,
		}
	}

	l := h.embeddedXray.GetLimiter()
	if l == nil {
		writeError(w, http.StatusInternalServerError, "Limiter not available")
		return
	}

	l.AddInboundLimiter(req.InboundTag, req.NodeLimit, users)

	if len(req.AutoSpeedRules) > 0 {
		if monitor := h.embeddedXray.GetSpeedMonitor(); monitor != nil {
			monitor.UpdateRules(req.AutoSpeedRules)
			monitor.SetLimiter(l)
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

// HandleSwitchXrayMode 处理 POST /api/child/agent/switch-xray-mode。
// 切换 xray_mode（embedded↔external），更新 config.yaml 并自重启 agent。
func (h *ManageHandler) HandleSwitchXrayMode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	var req struct {
		XrayMode string `json:"xray_mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if req.XrayMode != "external" && req.XrayMode != "embedded" {
		writeError(w, http.StatusBadRequest, "xray_mode must be 'external' or 'embedded'")
		return
	}

	if h.configPath == "" {
		writeError(w, http.StatusInternalServerError, "Config path not set")
		return
	}

	// 读取当前 config.yaml
	data, err := os.ReadFile(h.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Read config: %v", err))
		return
	}

	// 更新 xray_mode 行
	lines := strings.Split(string(data), "\n")
	found := false
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "xray_mode:") {
			lines[i] = "xray_mode: " + req.XrayMode
			found = true
			break
		}
	}
	if !found {
		lines = append(lines, "xray_mode: "+req.XrayMode)
	}

	if err := os.WriteFile(h.configPath, []byte(strings.Join(lines, "\n")), 0644); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Write config: %v", err))
		return
	}
	log.Printf("[Manage] Config updated: xray_mode=%s", req.XrayMode)

	// 切到 external：确保外部 xray 已安装并启用
	if req.XrayMode == "external" {
		if err := h.ensureExternalXray(); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Ensure external xray: %v", err))
			return
		}
	}

	// 先回复成功，再延迟自重启
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Switching to %s mode, agent restarting...", req.XrayMode),
	})

	// 刷出响应后再重启
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	// 不用 exec systemctl restart(同进程 fork 偶发不释放/不退出导致一个 PID 占多个端口);
	// 直接 os.Exit,systemd Restart=always 拉起新实例读新 xray_mode。
	go func() {
		time.Sleep(500 * time.Millisecond)
		log.Printf("[Manage] Exiting for xray_mode switch to %s (systemd will restart)", req.XrayMode)
		os.Exit(0)
	}()
}

// HandleSwitchListenPort 处理 POST /api/child/agent/switch-listen-port。
// 改 config.yaml 的 listen_port + 重启 agent。重启后 agent 用新端口监听,
// 主控下次重连按 server.ListenPort 自动用新端口。
func (h *ManageHandler) HandleSwitchListenPort(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	var req struct {
		ListenPort int `json:"listen_port"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	// 0 表示恢复默认(删除 listen_port 行,agent 用内置 23889);1024-65535 才作为有效端口
	if req.ListenPort != 0 && (req.ListenPort < 1024 || req.ListenPort > 65535) {
		writeError(w, http.StatusBadRequest, "listen_port must be 0 or in 1024-65535")
		return
	}
	if h.configPath == "" {
		writeError(w, http.StatusInternalServerError, "Config path not set")
		return
	}

	// 历史上这里用 net.Listen 做"预检",但同进程内打开+关闭监听存在 Go runtime / 内核
	// epoll 句柄残留的边界情况 — 一旦后续 systemctl 重启没成功(D-Bus / unit 名异常 / 静默 fail),
	// 老 agent 进程就会保留两个 LISTEN(原端口 + 预检端口),反映到外面就是 "同 PID 占两个端口"。
	// 改为"信任 + 重试":只校验数字范围,写 config,然后 os.Exit 让 systemd 拉起新实例;
	// 新实例 bind 失败由 main 里的 listenWithRetry 兜底(EADDRINUSE 重试 6 次每次 2s)。

	data, err := os.ReadFile(h.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Read config: %v", err))
		return
	}

	lines := strings.Split(string(data), "\n")
	out := make([]string, 0, len(lines)+1)
	found := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "listen_port:") {
			if req.ListenPort > 0 {
				out = append(out, fmt.Sprintf("listen_port: \"%d\"", req.ListenPort))
				found = true
			}
			// req.ListenPort == 0 时直接丢弃这一行(恢复默认)
			continue
		}
		out = append(out, line)
	}
	if !found && req.ListenPort > 0 {
		out = append(out, fmt.Sprintf("listen_port: \"%d\"", req.ListenPort))
	}

	if err := os.WriteFile(h.configPath, []byte(strings.Join(out, "\n")), 0644); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Write config: %v", err))
		return
	}
	log.Printf("[Manage] Config updated: listen_port=%d", req.ListenPort)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Listen port set to %d, agent restarting...", req.ListenPort),
	})
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	// 不用 exec.Command 跑 systemctl restart —— 同进程内 fork + 子进程继承 FD + systemctl 静默失败
	// 这一串组合会导致老 agent 没死,新端口又被预检/重启循环里某个步骤额外绑上,出现"同 PID 两个 LISTEN"。
	// 直接 os.Exit(0):systemd 的 Restart=always 会在 RestartSec 后拉起新实例,新实例读 config.yaml 的新端口绑定。
	go func() {
		time.Sleep(500 * time.Millisecond)
		log.Printf("[Manage] Exiting for listen_port switch to %d (systemd will restart)", req.ListenPort)
		os.Exit(0)
	}()
}

// HandleUpdateMasterURL 处理 POST /api/child/agent/update-master-url。
// 更新 config.yaml 中的 master_url 并重启 agent。
func (h *ManageHandler) HandleUpdateMasterURL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	var req struct {
		MasterURL string `json:"master_url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.MasterURL == "" {
		writeError(w, http.StatusBadRequest, "master_url required")
		return
	}

	if h.configPath == "" {
		writeError(w, http.StatusInternalServerError, "Config path not set")
		return
	}

	data, err := os.ReadFile(h.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Read config: %v", err))
		return
	}

	lines := strings.Split(string(data), "\n")
	found := false
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "master_url:") {
			lines[i] = "master_url: " + req.MasterURL
			found = true
			break
		}
	}
	if !found {
		lines = append(lines, "master_url: "+req.MasterURL)
	}

	if err := os.WriteFile(h.configPath, []byte(strings.Join(lines, "\n")), 0644); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Write config: %v", err))
		return
	}
	log.Printf("[Manage] Config updated: master_url=%s", req.MasterURL)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("master_url updated to %s, agent restarting...", req.MasterURL),
	})

	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	// 不用 exec systemctl restart;直接 os.Exit,systemd Restart=always 拉起新实例。
	go func() {
		time.Sleep(500 * time.Millisecond)
		log.Printf("[Manage] Exiting for master_url update (systemd will restart)")
		os.Exit(0)
	}()
}

// ensureExternalXray 确保外部 Xray 已安装、启用并启动。
func (h *ManageHandler) ensureExternalXray() error {
	// 检查 xray 二进制是否存在
	if _, err := exec.LookPath("xray"); err != nil {
		log.Printf("[Manage] External xray not found, installing...")
		cmd := exec.Command("bash", "-c",
			`bash -c "$(curl -L https://github.com/XTLS/Xray-install/raw/main/install-release.sh)" @ install`)
		cmd.Env = os.Environ()
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("install xray: %v (%s)", err, string(output))
		}
		log.Printf("[Manage] External xray installed")
	}

	// 启用并启动外部 xray 服务
	exec.Command("systemctl", "enable", "xray").Run()
	if err := exec.Command("systemctl", "start", "xray").Run(); err != nil {
		return fmt.Errorf("start xray service: %v", err)
	}
	log.Printf("[Manage] External xray service enabled and started")
	return nil
}

// classifyRulePriority 把单条 routing rule 按 marktag/outboundTag 归类成 5 个优先级,数字越小越靠前:
//
//	0 - 端口转发(outboundTag 以 "tunnel-" 开头),xray 按序匹配,必须置顶以免被下游 routed/warp/api 截胡
//	1 - user 私有 routed 出站(marktag = "routed:<shortID>:u<username>:<labelSlug>",4 段)
//	2 - admin 共享 routed 出站(marktag = "routed:<shortID>:<labelSlug>",3 段)
//	3 - 家宽常用 / 测速分流 两个快捷预设(marktag 白名单匹配)
//	4 - 其他规则(空 marktag、自定义、ban_bt / fix_openai / warp_anti_china 等)
//
// 段数判断稳:simpleSlug 把非 [a-z0-9-] 全换成 -,labelSlug 不含冒号,所以
// "routed:" 前缀 + ":" 段数 = 3 或 4 可唯一区分 admin vs user。
// 不依赖 ":u" 前缀 — 避免 admin 起 label 叫 "unlock" 时被误判成 user。
// outboundTag = "tunnel-*" 是 miaomiaowuX 端口转发的命名约定(见前端 inbound-wizard generateTunnelTag),
// 单独提到 priority 0 是因为 xray routing 按数组顺序短路匹配,这类规则若被 routed/warp 截胡,
// 端口转发的语义就丢了。注意 "tunnel-in" 是 inboundTag(伪装入站),不会出现在 outboundTag,排除安全。
func classifyRulePriority(rule map[string]interface{}) int {
	if rule == nil {
		return 4
	}
	if outboundTag, _ := rule["outboundTag"].(string); strings.HasPrefix(outboundTag, "tunnel-") {
		return 0
	}
	marktag, _ := rule["marktag"].(string)
	if strings.HasPrefix(marktag, "routed:") {
		parts := strings.Split(marktag, ":")
		switch len(parts) {
		case 4:
			return 1 // routed:<id>:u<user>:<label>
		case 3:
			return 2 // routed:<id>:<label>
		}
		// 段数不匹配:可能是老格式 / 异常 marktag,降级到 admin 等级,不当 user 优待
		return 2
	}
	switch marktag {
	case "home_broadband_warp", "speedtest_warp":
		return 3
	}
	return 4
}
