package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"mmw-agent/internal/agent"
	"mmw-agent/internal/constants"
)

// APIHandler 处理来自主控端的请求（拉取模式）。
type APIHandler struct {
	client      *agent.Client
	configToken string
}

// 创建 API 处理器。
func NewAPIHandler(client *agent.Client, configToken string) *APIHandler {
	return &APIHandler{
		client:      client,
		configToken: configToken,
	}
}

// 返回流量数据。
func (h *APIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !h.authenticate(r) {
		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Unauthorized",
		})
		return
	}

	stats, err := h.client.GetStats()
	if err != nil {
		log.Printf("[API] Failed to get stats: %v", err)
		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Failed to collect stats",
		})
		return
	}

	resp := map[string]interface{}{
		"success": true,
		"stats":   stats,
	}
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	json.NewEncoder(w).Encode(resp)
}

// 返回速率数据。
func (h *APIHandler) ServeSpeedHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !h.authenticate(r) {
		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Unauthorized",
		})
		return
	}

	uploadSpeed, downloadSpeed := h.client.GetSpeed()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":        true,
		"upload_speed":   uploadSpeed,
		"download_speed": downloadSpeed,
	})
}

// 校验请求身份（token + User-Agent）。
func (h *APIHandler) authenticate(r *http.Request) bool {
	if r.Header.Get(constants.HeaderUserAgent) != constants.AgentUserAgent {
		return false
	}

	if h.configToken == "" {
		return true
	}

	auth := r.Header.Get(constants.HeaderAuthorization)
	if auth == "" {
		return false
	}

	if strings.HasPrefix(auth, constants.BearerPrefix) {
		token := strings.TrimPrefix(auth, constants.BearerPrefix)
		return token == h.configToken
	}

	return auth == h.configToken
}
