package agent

import (
	"net"
	"strconv"
	"sync"
	"time"
)

// 伪装探针的省市 ping:从 agent 网络位置 TCP-connect 各省市三网目标测 RTT。
// 仿 domain_latency_handler.go 的 probeOneDomainLatency(无 ICMP,回避 raw socket)。

// ProbePingTarget 是主控下发的一个 ping 目标。
type ProbePingTarget struct {
	Key  string `json:"key"`
	Host string `json:"host"`
	Port int    `json:"port"`
}

// ProbeLatencySample 是一次 ping 结果,上报给主控。
type ProbeLatencySample struct {
	Key       string `json:"key"`
	Success   bool   `json:"success"`
	LatencyMs int64  `json:"latency_ms"`
}

const (
	probePingTimeout     = 3 * time.Second
	probePingConcurrency = 10 // 并发上限,防把 agent 网络打满
	probePingMaxTargets  = 30 // 目标数上限(与主控侧呼应)
)

// probeRegions 并发拨测一批目标,返回每个的延迟。并发受 probePingConcurrency 限流。
func probeRegions(targets []ProbePingTarget) []ProbeLatencySample {
	if len(targets) > probePingMaxTargets {
		targets = targets[:probePingMaxTargets]
	}
	out := make([]ProbeLatencySample, len(targets))
	sem := make(chan struct{}, probePingConcurrency)
	var wg sync.WaitGroup
	for i := range targets {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			out[idx] = pingOneTarget(targets[idx])
		}(i)
	}
	wg.Wait()
	return out
}

func pingOneTarget(t ProbePingTarget) ProbeLatencySample {
	port := t.Port
	if port <= 0 {
		port = 80
	}
	addr := net.JoinHostPort(t.Host, strconv.Itoa(port))
	start := time.Now()
	conn, err := net.DialTimeout("tcp", addr, probePingTimeout)
	if err != nil {
		return ProbeLatencySample{Key: t.Key, Success: false}
	}
	_ = conn.Close()
	return ProbeLatencySample{Key: t.Key, Success: true, LatencyMs: time.Since(start).Milliseconds()}
}
