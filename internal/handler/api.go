package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"mmw-agent/internal/agent"
	"mmw-agent/internal/config"
)

// APIHandler handles API requests from the master server (for pull mode)
type APIHandler struct {
	client      *agent.Client
	configToken string
}

// NewAPIHandler creates a new API handler
func NewAPIHandler(client *agent.Client, configToken string) *APIHandler {
	return &APIHandler{
		client:      client,
		configToken: configToken,
	}
}

// ServeHTTP handles the HTTP request for traffic data
func (h *APIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !h.authenticate(r) {
		w.Header().Set("Content-Type", "application/json")
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
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Failed to collect stats",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"stats":   stats,
	})
}

// ServeSpeedHTTP handles the HTTP request for speed data
func (h *APIHandler) ServeSpeedHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !h.authenticate(r) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Unauthorized",
		})
		return
	}

	uploadSpeed, downloadSpeed := h.client.GetSpeed()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":        true,
		"upload_speed":   uploadSpeed,
		"download_speed": downloadSpeed,
	})
}

// authenticate checks if the request is authorized (token + User-Agent)
func (h *APIHandler) authenticate(r *http.Request) bool {
	if r.Header.Get("User-Agent") != config.AgentUserAgent {
		return false
	}

	if h.configToken == "" {
		return true
	}

	auth := r.Header.Get("Authorization")
	if auth == "" {
		return false
	}

	if strings.HasPrefix(auth, "Bearer ") {
		token := strings.TrimPrefix(auth, "Bearer ")
		return token == h.configToken
	}

	return auth == h.configToken
}
