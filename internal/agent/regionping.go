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
	// Type 是探测方式:"icmp" 走 ICMP echo,其余(含空)走 TCP connect。
	// 空=tcp 是为了兼容老主控下发的目标(它不带这个字段)。
	Type string `json:"type,omitempty"`
}

// ProbeLatencySample 是一次 ping 结果,上报给主控。
type ProbeLatencySample struct {
	Key       string `json:"key"`
	Success   bool   `json:"success"`
	LatencyMs int64  `json:"latency_ms"`
	// At 是**采样时刻**(unix 秒)。必须由 agent 打,不能让主控用收包时间代替:
	// 上报是搭 traffic tick(5s)的车走的,与 ping 周期不同频,用收包时间会把
	// 时间轴压缩成 5s 一格,ring 的有效覆盖窗口随之缩水。老 agent 不发这个字段,
	// 主控侧对 0 值回落到接收时刻。
	At int64 `json:"at,omitempty"`
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
	at := time.Now().Unix() // 一轮共用一个采样时刻,便于主控按 (key, at) 去重
	sem := make(chan struct{}, probePingConcurrency)
	var wg sync.WaitGroup
	for i := range targets {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			s := pingOneTarget(targets[idx])
			s.At = at
			out[idx] = s
		}(i)
	}
	wg.Wait()
	return out
}

func pingOneTarget(t ProbePingTarget) ProbeLatencySample {
	// ICMP 目标:环境不支持(非 root 且 ping_group_range 没放开)时静默降级到 TCP,
	// 而不是把这个目标标成失败 —— 那会让面板显示成"目标不通",误导排查方向。
	if t.Type == "icmp" && icmpAvailable() {
		rtt, err := icmpPing(t.Host, probePingTimeout)
		if err != nil {
			return ProbeLatencySample{Key: t.Key, Success: false}
		}
		return ProbeLatencySample{Key: t.Key, Success: true, LatencyMs: rtt.Milliseconds()}
	}

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
