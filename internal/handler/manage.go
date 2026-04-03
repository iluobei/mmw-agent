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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mmw-agent/internal/config"
	"mmw-agent/internal/xrpc"

	"github.com/xtls/xray-core/app/proxyman/command"
	"github.com/xtls/xray-core/infra/conf"
)

var nginxInstalling atomic.Bool

// ManageHandler handles management API requests for child servers
type ManageHandler struct {
	configToken string
}

// NewManageHandler creates a new management handler
func NewManageHandler(configToken string) *ManageHandler {
	return &ManageHandler{
		configToken: configToken,
	}
}

// authenticate checks if the request is authorized (token + User-Agent)
func (h *ManageHandler) authenticate(r *http.Request) bool {
	if r.Header.Get("User-Agent") != config.AgentUserAgent {
		return false
	}

	if h.configToken == "" {
		return true
	}

	auth := r.Header.Get("Authorization")
	if auth == "" {
		auth = r.Header.Get("MM-Remote-Token")
	}
	if auth == "" {
		return false
	}

	if strings.HasPrefix(auth, "Bearer ") {
		token := strings.TrimPrefix(auth, "Bearer ")
		return token == h.configToken
	}

	return auth == h.configToken
}

// writeJSON writes JSON response
func writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// writeError writes error response
func writeError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, map[string]interface{}{
		"success": false,
		"error":   message,
	})
}

// ================== System Services Status ==================

// ServicesStatusResponse represents the response for services status
type ServicesStatusResponse struct {
	Success bool           `json:"success"`
	Xray    *ServiceStatus `json:"xray,omitempty"`
	Nginx   *ServiceStatus `json:"nginx,omitempty"`
}

// ServiceStatus represents a service status
type ServiceStatus struct {
	Installed bool   `json:"installed"`
	Running   bool   `json:"running"`
	Version   string `json:"version,omitempty"`
}

// HandleServicesStatus handles GET /api/child/services/status
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

	xrayPath, err := exec.LookPath("xray")
	if err != nil {
		commonPaths := []string{"/usr/local/bin/xray", "/usr/bin/xray", "/opt/xray/xray"}
		for _, p := range commonPaths {
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

	// Check systemctl first
	cmd := exec.Command("systemctl", "is-active", "xray")
	output, _ := cmd.Output()
	if strings.TrimSpace(string(output)) == "active" {
		status.Running = true
		return status
	}

	// Fallback: check if xray process is running via pgrep
	pgrepCmd := exec.Command("pgrep", "-x", "xray")
	if err := pgrepCmd.Run(); err == nil {
		status.Running = true
		return status
	}

	// Fallback: check via ps for processes containing "xray"
	psCmd := exec.Command("bash", "-c", "ps aux | grep -v grep | grep -E '[x]ray' | head -1")
	if output, err := psCmd.Output(); err == nil && len(strings.TrimSpace(string(output))) > 0 {
		status.Running = true
	}

	return status
}

func (h *ManageHandler) getNginxStatus() *ServiceStatus {
	status := &ServiceStatus{}

	// Check PATH first, then compiled install path
	nginxPath, err := exec.LookPath("nginx")
	if err != nil {
		if _, statErr := os.Stat("/usr/local/nginx/sbin/nginx"); statErr == nil {
			nginxPath = "/usr/local/nginx/sbin/nginx"
			err = nil
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

	// Check systemctl first
	cmd := exec.Command("systemctl", "is-active", "nginx")
	output, _ := cmd.Output()
	if strings.TrimSpace(string(output)) == "active" {
		status.Running = true
		return status
	}

	// Fallback: check if nginx process is running via pgrep
	pgrepCmd := exec.Command("pgrep", "-x", "nginx")
	if err := pgrepCmd.Run(); err == nil {
		status.Running = true
		return status
	}

	// Fallback: check via ps for nginx master process
	psCmd := exec.Command("bash", "-c", "ps aux | grep -v grep | grep -E 'nginx: master' | head -1")
	if output, err := psCmd.Output(); err == nil && len(strings.TrimSpace(string(output))) > 0 {
		status.Running = true
	}

	return status
}

// ================== Service Control ==================

// ServiceControlRequest represents a service control request
type ServiceControlRequest struct {
	Service string `json:"service"`
	Action  string `json:"action"`
}

// HandleServiceControl handles POST /api/child/services/control
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

	cmd := exec.Command("systemctl", req.Action, req.Service)
	output, err := cmd.CombinedOutput()
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to %s %s: %v - %s", req.Action, req.Service, err, string(output)))
		return
	}

	log.Printf("[Manage] Service %s: %s", req.Service, req.Action)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Service %s %sed successfully", req.Service, req.Action),
	})
}

// ================== Xray Installation ==================

// HandleXrayInstall handles POST /api/child/xray/install
func (h *ManageHandler) HandleXrayInstall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
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

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Xray installed successfully",
		"output":  stdout.String(),
	})
}

// HandleXrayRemove handles POST /api/child/xray/remove
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

// ================== Xray Configuration ==================

// HandleXrayConfig handles GET/POST /api/child/xray/config
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
	configPaths := []string{
		"/usr/local/etc/xray/config.json",
		"/etc/xray/config.json",
		"/opt/xray/config.json",
	}

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
		writeError(w, http.StatusNotFound, "Xray config not found")
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
		configPath = "/usr/local/etc/xray/config.json"
	}

	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create directory: %v", err))
		return
	}

	if err := os.WriteFile(configPath, []byte(req.Config), 0644); err != nil {
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

// ================== Xray System Configuration ==================

// XraySystemConfig represents the system configuration state
type XraySystemConfig struct {
	MetricsEnabled bool   `json:"metrics_enabled"`
	MetricsListen  string `json:"metrics_listen"`
	StatsEnabled   bool   `json:"stats_enabled"`
	GrpcEnabled    bool   `json:"grpc_enabled"`
	GrpcPort       int    `json:"grpc_port"`
}

// HandleXraySystemConfig handles GET/POST /api/child/xray/system-config
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
		MetricsListen: "127.0.0.1:38889",
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
					"listen":   "127.0.0.1",
					"protocol": "dokodemo-door",
					"settings": map[string]interface{}{
						"address": "127.0.0.1",
					},
				}
				config["inbounds"] = append([]interface{}{apiInbound}, inbounds...)
			}
		} else {
			config["inbounds"] = []interface{}{
				map[string]interface{}{
					"tag":      "api",
					"port":     float64(req.GrpcPort),
					"listen":   "127.0.0.1",
					"protocol": "dokodemo-door",
					"settings": map[string]interface{}{
						"address": "127.0.0.1",
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

	cmd := exec.Command("systemctl", "restart", "xray")
	if err := cmd.Run(); err != nil {
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

// ================== Nginx Installation ==================

// HandleNginxInstall handles POST /api/child/nginx/install
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
	}()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Nginx installation started, please check status later",
	})
}

// HandleNginxRemove handles POST /api/child/nginx/remove
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

// ================== Nginx Configuration ==================

// HandleNginxConfig handles GET/POST /api/child/nginx/config
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
	configPaths := []string{
		"/etc/nginx/nginx.conf",
		"/usr/local/nginx/conf/nginx.conf",
	}

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
		configPath = "/etc/nginx/nginx.conf"
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

// ================== System Info ==================

// HandleSystemInfo handles GET /api/child/system/info
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
		"success": true,
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

// ================== Config Files Management ==================

// ConfigFileInfo represents a config file entry
type ConfigFileInfo struct {
	Name    string `json:"name"`
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	ModTime string `json:"mod_time"`
}

// HandleXrayConfigFiles handles listing and managing xray config files
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
	configDirs := []string{
		"/usr/local/etc/xray",
		"/etc/xray",
		"/opt/xray",
	}

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
		"/usr/local/etc/xray",
		"/etc/xray",
		"/opt/xray",
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
		"/usr/local/etc/xray",
		"/etc/xray",
		"/opt/xray",
	}

	var configDir string
	for _, dir := range configDirs {
		if _, err := os.Stat(dir); err == nil {
			configDir = dir
			break
		}
	}

	if configDir == "" {
		configDir = "/usr/local/etc/xray"
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

// HandleNginxConfigFiles handles listing and managing nginx config files
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
		{"/etc/nginx", "main"},
		{"/etc/nginx/sites-available", "sites-available"},
		{"/etc/nginx/sites-enabled", "sites-enabled"},
		{"/etc/nginx/conf.d", "conf.d"},
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
		"/etc/nginx",
		"/etc/nginx/sites-available",
		"/etc/nginx/sites-enabled",
		"/etc/nginx/conf.d",
		"/usr/local/nginx/conf",
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
		"/etc/nginx",
		"/usr/local/nginx/conf",
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

// ================== Xray Inbounds Management ==================

// InboundRequest represents inbound management request
type InboundRequest struct {
	Action  string                 `json:"action"`
	Inbound map[string]interface{} `json:"inbound,omitempty"`
	Tag     string                 `json:"tag,omitempty"`
}

// HandleInbounds handles inbound management
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
	configInbounds := h.getInboundsFromConfig()
	runtimeTags := h.getInboundTagsFromGRPC()
	mergedInbounds := h.mergeInbounds(configInbounds, runtimeTags)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"inbounds": mergedInbounds,
	})
}

func (h *ManageHandler) getInboundsFromConfig() []map[string]interface{} {
	configPath := h.findXrayConfigPath()
	if configPath == "" {
		return nil
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		log.Printf("[Manage] Failed to read config file: %v", err)
		return nil
	}

	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		log.Printf("[Manage] Failed to parse config: %v", err)
		return nil
	}

	rawInbounds, _ := config["inbounds"].([]interface{})
	inbounds := make([]map[string]interface{}, 0, len(rawInbounds))
	for _, ib := range rawInbounds {
		if ibMap, ok := ib.(map[string]interface{}); ok {
			inbounds = append(inbounds, ibMap)
		}
	}
	return inbounds
}

func (h *ManageHandler) getInboundTagsFromGRPC() []string {
	apiPort := h.findXrayAPIPort()
	if apiPort == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clients, err := xrpc.New(ctx, "127.0.0.1", uint16(apiPort))
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

	apiPort := h.findXrayAPIPort()
	if apiPort == 0 {
		writeError(w, http.StatusInternalServerError, "Xray API not available")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clients, err := xrpc.New(ctx, "127.0.0.1", uint16(apiPort))
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

		// Try to remove from runtime (ignore error if not running)
		runtimeErr := h.removeInbound(ctx, clients.Handler, req.Tag)
		if runtimeErr != nil {
			log.Printf("[Manage] Warning: Failed to remove inbound from runtime: %v", runtimeErr)
		}

		// Remove from config file (this is the primary operation)
		configErr := h.removeInboundFromConfig(req.Tag)
		if configErr != nil {
			log.Printf("[Manage] Warning: Failed to remove inbound from config: %v", configErr)
		}

		// Success if config operation succeeded (runtime removal is optional)
		// The inbound might not exist in runtime if Xray wasn't restarted after config change
		if configErr != nil {
			// Config file operation failed
			if runtimeErr != nil {
				// Both failed - check if it's just "not found" errors
				if strings.Contains(runtimeErr.Error(), "not enough information") {
					// Xray says the inbound doesn't exist in runtime, which is fine
					// Just report config error
					writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to remove inbound from config: %v", configErr))
				} else {
					writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to remove inbound: runtime=%v, config=%v", runtimeErr, configErr))
				}
			} else {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to remove inbound from config: %v", configErr))
			}
			return
		}

		// Config succeeded, runtime error is acceptable (inbound might not be loaded)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Inbound removed successfully",
		})

	default:
		writeError(w, http.StatusBadRequest, "Invalid action. Must be 'add' or 'remove'")
	}
}

// ================== Xray Outbounds Management ==================

// OutboundRequest represents outbound management request
type OutboundRequest struct {
	Action   string                 `json:"action"`
	Outbound map[string]interface{} `json:"outbound,omitempty"`
	Tag      string                 `json:"tag,omitempty"`
}

// HandleOutbounds handles outbound management
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

	clients, err := xrpc.New(ctx, "127.0.0.1", uint16(apiPort))
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

	default:
		writeError(w, http.StatusBadRequest, "Invalid action. Must be 'add' or 'remove'")
	}
}

// ================== Xray Routing Management ==================

// RoutingRequest represents routing management request
type RoutingRequest struct {
	Action  string                 `json:"action"`
	Routing map[string]interface{} `json:"routing,omitempty"`
	Rule    map[string]interface{} `json:"rule,omitempty"`
	Index   int                    `json:"index,omitempty"`
}

// HandleRouting handles routing management
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
		rules = append(rules, req.Rule)
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

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Routing updated successfully. Restart Xray to apply changes.",
	})
}

// ================== Helper Functions ==================

func (h *ManageHandler) findXrayConfigPath() string {
	configPaths := []string{
		"/usr/local/etc/xray/config.json",
		"/etc/xray/config.json",
		"/opt/xray/config.json",
	}

	for _, p := range configPaths {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
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

	inbounds, _ := config["inbounds"].([]interface{})
	inbounds = append(inbounds, inbound)
	config["inbounds"] = inbounds

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
		if ibTag != tag {
			newInbounds = append(newInbounds, ib)
		}
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

// ================== Scan ==================

// ScanResponse represents the response for scan operation
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

// HandleScan handles POST /api/child/scan
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
		cmd := exec.Command("systemctl", "restart", "xray")
		if err := cmd.Run(); err != nil {
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

// ================== Xray Config Auto-Complete ==================

// EnsureXrayConfigResult holds the result of config check
type EnsureXrayConfigResult struct {
	ConfigPath    string   `json:"config_path"`
	Modified      bool     `json:"modified"`
	AddedSections []string `json:"added_sections,omitempty"`
	Error         string   `json:"error,omitempty"`
}

// EnsureXrayConfig checks and completes Xray configuration
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

	if _, ok := config["metrics"]; !ok {
		config["metrics"] = map[string]interface{}{
			"tag":    "Metrics",
			"listen": "127.0.0.1:38889",
		}
		result.AddedSections = append(result.AddedSections, "metrics")
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
		"listen":   "127.0.0.1",
		"protocol": "dokodemo-door",
		"settings": map[string]interface{}{
			"address": "127.0.0.1",
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

// ================== Certificate Deploy ==================

// CertDeployRequest represents a certificate deploy request from master
type CertDeployRequest struct {
	Domain   string `json:"domain"`
	CertPEM  string `json:"cert_pem"`
	KeyPEM   string `json:"key_pem"`
	CertPath string `json:"cert_path"`
	KeyPath  string `json:"key_path"`
	Reload   string `json:"reload"` // nginx, xray, both, none
}

// HandleCertDeploy handles POST /api/child/cert/deploy
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
		return runCommand("nginx", "-s", "reload")
	case "xray":
		return runCommand("systemctl", "restart", "xray")
	case "both":
		if err := runCommand("nginx", "-s", "reload"); err != nil {
			return err
		}
		return runCommand("systemctl", "restart", "xray")
	}
	return nil
}

func runCommand(name string, args ...string) error {
	if output, err := exec.Command(name, args...).CombinedOutput(); err != nil {
		return fmt.Errorf("%s: %s: %w", name, string(output), err)
	}
	return nil
}

// ================== SSE Streaming Install/Remove ==================

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
	log.Printf("[Manage] Starting Nginx install (stream)...")
	cmd := exec.CommandContext(r.Context(), "bash", "-c",
		`curl -fsSL https://raw.githubusercontent.com/iluobei/miaomiaowuX/main/install-nginx.sh | bash`)
	cmd.Env = os.Environ()
	sseStreamCmd(w, r, cmd, "Nginx installed successfully")
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
