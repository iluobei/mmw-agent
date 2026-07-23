package handler

import (
	"encoding/json"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"mmw-agent/internal/agent"
	"mmw-agent/internal/constants"
)

type DomainLatencyProbeRequest struct {
	Domains   []string `json:"domains"`
	TimeoutMs int      `json:"timeout_ms,omitempty"`
	// AllowICMP:所有候选端口都拨不通时,再用 ICMP 探一次主机可达性。
	// 用于"目标机上没有任何已知在监听的端口"的场景(全新服务器、agent 开了端口隐身)——
	// 那时 TCP 失败并不代表网络不通,不降级就只能给出误导性的"不通"。
	// 老主控不带这个字段 → 默认 false → 行为与之前完全一致。
	AllowICMP bool `json:"allow_icmp,omitempty"`
}

type DomainLatencyProbeResult struct {
	Domain    string `json:"domain"`
	Target    string `json:"target"`
	Success   bool   `json:"success"`
	LatencyMs int64  `json:"latency_ms,omitempty"`
	Error     string `json:"error,omitempty"`
	// Method 标明这条结果是怎么测出来的:"tcp"(默认)或 "icmp"(TCP 全失败后的降级)。
	// 主控据此在 UI 上区分"端口通"与"主机通但端口未验证"。
	Method string `json:"method,omitempty"`
}

// 处理 POST /api/child/domains/latency 请求。
func (h *ManageHandler) HandleDomainLatencyProbe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	var req DomainLatencyProbeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if len(req.Domains) == 0 {
		writeError(w, http.StatusBadRequest, "domains is required")
		return
	}

	timeoutMs := req.TimeoutMs
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

	domains := uniqueProbeDomains(req.Domains)
	if len(domains) == 0 {
		writeError(w, http.StatusBadRequest, "no valid domain to probe")
		return
	}
	if len(domains) > constants.DomainProbeMaxCount {
		domains = domains[:constants.DomainProbeMaxCount]
	}

	results := make([]DomainLatencyProbeResult, 0, len(domains))
	resultCh := make(chan DomainLatencyProbeResult, len(domains))
	sem := make(chan struct{}, constants.DomainProbeConcurrency)
	var wg sync.WaitGroup

	for _, domain := range domains {
		wg.Add(1)
		domain := domain
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			resultCh <- probeOneDomainLatency(domain, timeout, req.AllowICMP)
		}()
	}

	wg.Wait()
	close(resultCh)

	for result := range resultCh {
		results = append(results, result)
	}

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

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"results": results,
		"count":   len(results),
	})
}

func uniqueProbeDomains(rawDomains []string) []string {
	out := make([]string, 0, len(rawDomains))
	seen := make(map[string]struct{}, len(rawDomains))
	for _, raw := range rawDomains {
		normalized := normalizeProbeInput(raw)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func normalizeProbeInput(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}

	if strings.Contains(s, "://") {
		if idx := strings.Index(s, "://"); idx >= 0 && idx+3 < len(s) {
			s = s[idx+3:]
		}
	}

	if idx := strings.Index(s, "/"); idx >= 0 {
		s = s[:idx]
	}

	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")

	return s
}

func probeOneDomainLatency(domain string, timeout time.Duration, allowICMP bool) DomainLatencyProbeResult {
	host := domain
	port := "443"

	if h, p, ok := splitHostPortLoose(domain); ok {
		host = h
		port = p
	}

	if host == "" {
		return DomainLatencyProbeResult{
			Domain:  domain,
			Target:  domain,
			Success: false,
			Error:   "empty host",
		}
	}

	target := net.JoinHostPort(host, port)
	start := time.Now()
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err == nil {
		_ = conn.Close()
		return DomainLatencyProbeResult{
			Domain:    host,
			Target:    target,
			Success:   true,
			LatencyMs: time.Since(start).Milliseconds(),
			Method:    "tcp",
		}
	}

	// TCP 不通 → 按需降级 ICMP:回答"主机是否可达"而不是"这个端口是否开着"。
	// 环境不支持发 ICMP(非 root 且 ping_group_range 没放开)时保持 TCP 结论,
	// 不把"发不出 ICMP"误报成别的东西。
	if allowICMP && agent.ICMPAvailable() {
		if rtt, ierr := agent.ICMPPing(host, timeout); ierr == nil {
			return DomainLatencyProbeResult{
				Domain:    host,
				Target:    target,
				Success:   true,
				LatencyMs: rtt.Milliseconds(),
				Method:    "icmp",
			}
		}
	}

	return DomainLatencyProbeResult{
		Domain:  host,
		Target:  target,
		Success: false,
		Error:   err.Error(),
		Method:  "tcp",
	}
}

func splitHostPortLoose(input string) (host string, port string, ok bool) {
	s := strings.TrimSpace(input)
	if s == "" {
		return "", "", false
	}

	if h, p, err := net.SplitHostPort(s); err == nil {
		if h != "" && p != "" {
			return h, p, true
		}
	}

	// 兼容不带方括号的 "domain:443" 写法。
	idx := strings.LastIndex(s, ":")
	if idx <= 0 || idx >= len(s)-1 {
		return "", "", false
	}
	h := s[:idx]
	p := s[idx+1:]
	if _, err := strconv.Atoi(p); err != nil {
		return "", "", false
	}
	return h, p, true
}
