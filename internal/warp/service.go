// service.go — WARP 状态机 + 持久化 + Xray outbound JSON 生成。
//
// 一个 agent 一份状态(warp.json),路径由 NewService 传入(通常是 agent 工作目录)。
// 安装 → 注册 Cloudflare + 持久化 + 生成 warp-v4/warp-v6 双 outbound JSON。

package warp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// State 持久化到 warp.json 的完整凭证 + 配置。
// 私钥/access_token 都是敏感凭证,文件权限 0600。
type State struct {
	PrivateKey   string    `json:"private_key"`           // base64
	PublicKey    string    `json:"public_key"`            // base64
	DeviceID     string    `json:"device_id"`             // Cloudflare 分配
	AccessToken  string    `json:"access_token"`          // Cloudflare 分配,Bearer 鉴权用
	LicenseKey   string    `json:"license_key,omitempty"` // WARP+ license(免费等同免费 license)
	ClientID     string    `json:"client_id"`             // base64,前 3 字节作 reserved
	AddrV4       string    `json:"addr_v4,omitempty"`     // 例 172.16.0.2(无 prefix)
	AddrV6       string    `json:"addr_v6,omitempty"`     // 例 2606:4700:110:xxxx::xxxx
	PeerPubKey   string    `json:"peer_public_key"`       // Cloudflare 给的 wg server pub
	PeerEndpoint string    `json:"peer_endpoint"`         // 例 162.159.193.10:2408
	RegisteredAt time.Time `json:"registered_at"`
}

// Service 持有 state 缓存 + 持久化路径。线程安全(mu 锁)。
type Service struct {
	mu        sync.RWMutex
	path      string // warp.json 持久化路径
	state     *State // nil 表示未注册
}

// NewService 用指定工作目录初始化;启动时尝试从磁盘加载状态。
// 加载失败(文件不存在/JSON 错)→ 视为未注册,后续调用 EnsureRegistered 会自动注册。
func NewService(workDir string) *Service {
	s := &Service{path: filepath.Join(workDir, "warp.json")}
	_ = s.load()
	return s
}

// IsInstalled 是否已注册(供 heartbeat 上报)。
func (s *Service) IsInstalled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state != nil && s.state.DeviceID != ""
}

// HasLicense 是否已激活 WARP+(license_key 非空且不等于免费默认)。
func (s *Service) HasLicense() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state != nil && s.state.LicenseKey != ""
}

// State 返回当前状态的快照(深拷贝,避免外部修改影响内部)。返回 nil 表示未注册。
func (s *Service) State() *State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.state == nil {
		return nil
	}
	cp := *s.state
	return &cp
}

// EnsureRegistered 幂等注册。已注册 → 直接返回缓存 state;未注册 → 生成密钥对 + 调
// Cloudflare API + 持久化。
func (s *Service) EnsureRegistered(ctx context.Context) (*State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != nil && s.state.DeviceID != "" {
		cp := *s.state
		return &cp, nil
	}

	privB64, pubB64, err := GenerateKeypair()
	if err != nil {
		return nil, fmt.Errorf("warp: generate keypair: %w", err)
	}
	resp, err := Register(ctx, pubB64)
	if err != nil {
		return nil, fmt.Errorf("warp: register: %w", err)
	}

	st := &State{
		PrivateKey:   privB64,
		PublicKey:    pubB64,
		DeviceID:     resp.ID,
		AccessToken:  resp.Token,
		LicenseKey:   resp.Account.License,
		ClientID:     resp.Config.ClientID,
		AddrV4:       resp.Config.Interface.Addresses.V4,
		AddrV6:       resp.Config.Interface.Addresses.V6,
		PeerPubKey:   firstPeerPubKey(resp),
		PeerEndpoint: firstPeerEndpoint(resp),
		RegisteredAt: time.Now(),
	}
	if err := s.saveLocked(st); err != nil {
		return nil, err
	}
	s.state = st
	cp := *st
	return &cp, nil
}

// RefreshConfig 拉一次最新 config(用于升级 WARP+ 后刷新 peer 等)。
// 不会重新生成 keypair,只更新 addresses / peer 信息。
func (s *Service) RefreshConfig(ctx context.Context) (*State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == nil || s.state.DeviceID == "" {
		return nil, errors.New("warp: not registered")
	}
	resp, err := GetConfig(ctx, s.state.DeviceID, s.state.AccessToken)
	if err != nil {
		return nil, fmt.Errorf("warp: get config: %w", err)
	}
	st := *s.state
	st.LicenseKey = resp.Account.License
	st.ClientID = resp.Config.ClientID
	st.AddrV4 = resp.Config.Interface.Addresses.V4
	st.AddrV6 = resp.Config.Interface.Addresses.V6
	st.PeerPubKey = firstPeerPubKey(resp)
	st.PeerEndpoint = firstPeerEndpoint(resp)
	if err := s.saveLocked(&st); err != nil {
		return nil, err
	}
	s.state = &st
	cp := st
	return &cp, nil
}

// SetLicense 升级 WARP+ + 立即刷新配置(license 改变后 Cloudflare 可能调整 peer)。
func (s *Service) SetLicense(ctx context.Context, license string) (*State, error) {
	s.mu.Lock()
	if s.state == nil {
		s.mu.Unlock()
		return nil, errors.New("warp: not registered")
	}
	deviceID, accessToken := s.state.DeviceID, s.state.AccessToken
	s.mu.Unlock()

	if err := UpdateLicense(ctx, deviceID, accessToken, license); err != nil {
		return nil, fmt.Errorf("warp: update license: %w", err)
	}
	return s.RefreshConfig(ctx)
}

// Uninstall 注销 Cloudflare 账号 + 删本地状态文件。
// 即使 Cloudflare 端调用失败也清本地(避免用户被 stuck)。
func (s *Service) Uninstall(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == nil {
		return nil
	}
	_ = Delete(ctx, s.state.DeviceID, s.state.AccessToken)
	s.state = nil
	if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("warp: remove state file: %w", err)
	}
	return nil
}

// BuildOutbounds 返回 warp-v4 + warp-v6 双 outbound 的 map(可直接喂给 ManageHandler.addOutbound)。
// 用户原话的 JSON 结构,只差 tag 和 domainStrategy。
func (s *Service) BuildOutbounds() ([]map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.state == nil {
		return nil, errors.New("warp: not registered")
	}
	reserved, err := reservedFromClientID(s.state.ClientID)
	if err != nil {
		return nil, fmt.Errorf("warp: reserved: %w", err)
	}
	// 把 [3]byte → []int(JSON 数字数组,xray-core conf 解析能接受)
	reservedInts := []int{int(reserved[0]), int(reserved[1]), int(reserved[2])}

	address := []string{}
	if s.state.AddrV4 != "" {
		address = append(address, s.state.AddrV4+"/32")
	}
	if s.state.AddrV6 != "" {
		address = append(address, s.state.AddrV6+"/128")
	}

	// builder 每次分配独立 map — 之前用 copyMap 浅拷,settings 是同一 map 引用,
	// 修 v4 的 settings.domainStrategy 会污染 v6,埋了隐藏 bug。改成 closure 直接生成两份独立。
	//
	// domainStrategy 必须放在 settings 里(xray wireguard outbound 的字段),不是 outbound 根属性。
	// 之前的实现把它写在根属性 → xray 启动时这个字段被忽略,settings 里的 ForceIP 沿用 →
	// 实际行为是 "解析后随便用任一 IP",拿不到 v4-only / v6-only 优先级控制 → 跨 v4/v6 流量混用。
	build := func(tag, domainStrategy string) map[string]any {
		return map[string]any{
			"tag":      tag,
			"protocol": "wireguard",
			"settings": map[string]any{
				"secretKey": s.state.PrivateKey,
				"address":   address,
				"peers": []map[string]any{
					{
						"publicKey": s.state.PeerPubKey,
						"endpoint":  s.state.PeerEndpoint,
					},
				},
				"reserved": reservedInts,
				// MTU 1420 = WireGuard 默认值,Cloudflare WARP 推荐值
				"mtu": 1420,
				// noKernelTun: false 强制用 userspace gVisor TUN — 不依赖宿主机 tun 模块 / CAP_NET_ADMIN。
				// 默认行为 (true) 在多数 VPS 上会让 wireguard outbound silently 失败:xray accept 进 outbound
				// 但 wg 内部 dial UDP 因权限不够卡住,不报错,流量空转。3x-ui WarpModal.tsx 同款显式 false。
				"noKernelTun": false,
				// wg 模块内部解析 Cloudflare endpoint(engage.cloudflareclient.com:2408)时的策略:
				// ForceIPv4v6 = v4 优先 → v6 fallback;ForceIPv6v4 = v6 优先 → v4 fallback。
				// 跟 outbound tag 配套:warp-v4 给纯 v4 客户端 / warp-v6 给纯 v6 / dual-stack 走对应优先。
				"domainStrategy": domainStrategy,
			},
		}
	}

	return []map[string]any{
		build("warp-v4", "ForceIPv4v6"),
		build("warp-v6", "ForceIPv6v4"),
	}, nil
}

// ---------- 持久化 ----------

func (s *Service) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return err
	}
	s.mu.Lock()
	s.state = &st
	s.mu.Unlock()
	return nil
}

func (s *Service) saveLocked(st *State) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("warp: mkdir state dir: %w", err)
	}
	// 0600 — 私钥 + access_token 不让其他用户读
	if err := os.WriteFile(s.path, data, 0o600); err != nil {
		return fmt.Errorf("warp: write state: %w", err)
	}
	return nil
}

// ---------- helpers ----------

func firstPeerPubKey(r *RegisterResp) string {
	if len(r.Config.Peers) > 0 {
		return r.Config.Peers[0].PublicKey
	}
	return ""
}

func firstPeerEndpoint(r *RegisterResp) string {
	if len(r.Config.Peers) > 0 {
		return r.Config.Peers[0].Endpoint.Host
	}
	return ""
}

func copyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
