package agent

import (
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

// ICMP echo 探测。与 regionping.go 的 TCP connect 并列,由目标的 Type 字段选择。
//
// 为什么要有它:TCP connect 测的是"建连耗时",受目标端口是否开放、是否被中间设备
// RST 影响;对 1.1.1.1 这类 anycast DNS,ICMP 更接近用户认知里的"ping"。
//
// 权限:agent 由 systemd 以 root 运行(unit 里没有 User=,也没收紧 capability),
// 所以 raw socket 通常可用。但为了不假设部署形态,这里做三级降级:
//   1. ip4:icmp / ip6:ipv6-icmp —— raw socket,需要 root 或 CAP_NET_RAW
//   2. udp4 / udp6 —— unprivileged ICMP,需要 net.ipv4.ping_group_range 覆盖当前 gid
//   3. 都不行 → 回落 TCP connect(由调用方处理)
//
// 探测结果缓存在 icmpMode,避免每次探测都试一遍 socket 创建。

type icmpAvailability int

const (
	icmpModeUnknown icmpAvailability = iota
	icmpModeRaw                      // ip4:icmp
	icmpModeUDP                      // udp4(unprivileged)
	icmpModeUnavailable
)

var (
	icmpModeOnce sync.Once
	icmpMode     icmpAvailability
	// echo ID 在 udp4 模式下会被内核改写,所以匹配只靠 seq + payload,不靠 ID。
	icmpSeq uint32
)

// detectICMPMode 试一次 socket 创建,结果全程缓存。
func detectICMPMode() icmpAvailability {
	icmpModeOnce.Do(func() {
		if c, err := icmp.ListenPacket("ip4:icmp", "0.0.0.0"); err == nil {
			_ = c.Close()
			icmpMode = icmpModeRaw
			return
		}
		if c, err := icmp.ListenPacket("udp4", "0.0.0.0"); err == nil {
			_ = c.Close()
			icmpMode = icmpModeUDP
			return
		}
		icmpMode = icmpModeUnavailable
	})
	return icmpMode
}

// icmpAvailable 供调用方判断是否要降级到 TCP。
func icmpAvailable() bool {
	m := detectICMPMode()
	return m == icmpModeRaw || m == icmpModeUDP
}

// icmpPing 发一个 ICMP echo 并等待回应,返回 RTT。
// host 可以是 IP 或域名(域名先解析)。
func icmpPing(host string, timeout time.Duration) (time.Duration, error) {
	mode := detectICMPMode()
	if mode == icmpModeUnavailable {
		return 0, errors.New("icmp unavailable")
	}

	ipAddr, err := net.ResolveIPAddr("ip", host)
	if err != nil {
		return 0, err
	}
	isV6 := ipAddr.IP.To4() == nil

	network := "ip4:icmp"
	listen := "0.0.0.0"
	if mode == icmpModeUDP {
		network, listen = "udp4", "0.0.0.0"
	}
	if isV6 {
		network, listen = "ip6:ipv6-icmp", "::"
		if mode == icmpModeUDP {
			network, listen = "udp6", "::"
		}
	}

	conn, err := icmp.ListenPacket(network, listen)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	msgType := icmp.Type(ipv4.ICMPTypeEcho)
	if isV6 {
		msgType = ipv6.ICMPTypeEchoRequest
	}

	seq := int(atomic.AddUint32(&icmpSeq, 1) & 0xffff)
	// payload 里放一个 magic,回包匹配时校验,避免收到别的进程的 echo reply。
	payload := []byte("mmw-agent-probe--")
	msg := icmp.Message{
		Type: msgType,
		Code: 0,
		Body: &icmp.Echo{ID: os.Getpid() & 0xffff, Seq: seq, Data: payload},
	}
	wb, err := msg.Marshal(nil)
	if err != nil {
		return 0, err
	}

	// udp4 模式下目标地址要用 *net.UDPAddr,raw 模式用 *net.IPAddr。
	var dst net.Addr = ipAddr
	if mode == icmpModeUDP {
		dst = &net.UDPAddr{IP: ipAddr.IP, Zone: ipAddr.Zone}
	}

	deadline := time.Now().Add(timeout)
	if err := conn.SetDeadline(deadline); err != nil {
		return 0, err
	}

	start := time.Now()
	if _, err := conn.WriteTo(wb, dst); err != nil {
		return 0, err
	}

	rb := make([]byte, 1500)
	proto := 1 // ICMPv4
	if isV6 {
		proto = 58
	}
	for {
		n, _, err := conn.ReadFrom(rb)
		if err != nil {
			return 0, err // 含超时
		}
		rm, err := icmp.ParseMessage(proto, rb[:n])
		if err != nil {
			continue
		}
		echo, ok := rm.Body.(*icmp.Echo)
		if !ok {
			continue
		}
		// 只认自己发的那一个:seq + payload magic。
		// 不校验 ID —— udp4 模式下内核会改写它。
		if echo.Seq != seq || string(echo.Data) != string(payload) {
			continue
		}
		if rm.Type == ipv4.ICMPTypeEchoReply || rm.Type == ipv6.ICMPTypeEchoReply {
			return time.Since(start), nil
		}
	}
}

// ICMPPing / ICMPAvailable 是 icmpPing / icmpAvailable 的导出包装,供 handler 包复用
// (domains/latency 在所有候选 TCP 端口都拨不通时降级到 ICMP)。
//
// 用包装而不是把原函数改成大写:regionping.go 等既有调用点保持不动,改动面最小。
func ICMPPing(host string, timeout time.Duration) (time.Duration, error) {
	return icmpPing(host, timeout)
}

// ICMPAvailable 报告本机能否发 ICMP(需要 root 或 ping_group_range 放开)。
// 不可用时调用方应保持 TCP 结论,不要把"发不出 ICMP"当成"目标不通"。
func ICMPAvailable() bool {
	return icmpAvailable()
}
