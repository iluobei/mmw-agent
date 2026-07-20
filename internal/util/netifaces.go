package util

import "net"

// NICAddr 是一个可用于 xray sendThrough 绑定的本机地址。
type NICAddr struct {
	IP     string `json:"ip"`     // 裸地址,不含掩码
	Family string `json:"family"` // v4 / v6
	CIDR   string `json:"cidr"`   // 带前缀长度,如 203.0.113.10/24
	Scope  string `json:"scope"`  // global / private / ula
}

// NIC 是一块启用中的网卡及其可绑定地址。
type NIC struct {
	Name  string    `json:"name"`
	Addrs []NICAddr `json:"addrs"`
}

// ListNICs 枚举本机可用于出站绑定的网卡地址。
// 只返回 UP 且非 loopback 接口上的地址,link-local 一律剔除(绑上去出不了网)。
//
// 注意:IPv6 隐私地址(temporary/deprecated)无法在这里识别 —— Go 的 net 包不暴露
// IFA_F_TEMPORARY/IFA_F_DEPRECATED,拿到它们要走 netlink。调用方(前端)需自行提示
// 用户:轮换的隐私地址一旦失效,绑了它的 sendThrough 就会失灵。
func ListNICs() ([]NIC, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	out := make([]NIC, 0, len(ifaces))
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		if picked := filterNICAddrs(addrs); len(picked) > 0 {
			out = append(out, NIC{Name: iface.Name, Addrs: picked})
		}
	}
	return out, nil
}

// filterNICAddrs 从一块网卡的地址里挑出可绑定的,并打上 scope 标签。
// 拆成纯函数是为了能脱离真实网卡做单测。
func filterNICAddrs(addrs []net.Addr) []NICAddr {
	out := make([]NICAddr, 0, len(addrs))
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP == nil {
			continue
		}
		ip := ipnet.IP
		// loopback 和 link-local 绑上去都发不出流量,直接剔除。
		if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
			continue
		}
		family := "v6"
		if ip.To4() != nil {
			family = "v4"
		}
		out = append(out, NICAddr{
			IP:     ip.String(),
			Family: family,
			CIDR:   ipnet.String(),
			Scope:  addrScope(ip),
		})
	}
	return out
}

// addrScope 区分公网地址与需要 NAT 的内网地址 —— NAT 后的机器绑私网地址是完全合法的,
// 所以私网地址要保留,只是让前端能标注出来。
func addrScope(ip net.IP) string {
	if ip.To4() == nil && len(ip) == net.IPv6len && ip[0]&0xfe == 0xfc {
		return "ula" // fc00::/7
	}
	if ip.IsPrivate() {
		return "private"
	}
	if ip4 := ip.To4(); ip4 != nil && ip4[0] == 100 && ip4[1]&0xc0 == 64 {
		return "private" // 100.64.0.0/10 CGNAT
	}
	return "global"
}
