package limiter

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"golang.org/x/time/rate"
)

// ipEntry 单个 IP 的元数据,带 lastSeen 用于 LRU 踢最旧。
type ipEntry struct {
	uid      int
	lastSeen time.Time
}

// emailIPMap 是 per-user 的 IP 表 + mutex。
// 设计:
//   - 用 map[ip]*ipEntry 替代之前的 sync.Map,需要遍历找 lastSeen 最旧的项,sync.Map 不擅长
//   - 内部加一把 mutex 串行化 read-modify-write(检查超限 → 踢最旧 → 加新),避免并发覆盖
type emailIPMap struct {
	mu sync.Mutex
	m  map[string]*ipEntry
}

func newEmailIPMap() *emailIPMap {
	return &emailIPMap{m: make(map[string]*ipEntry)}
}

type InboundInfo struct {
	Tag            string
	NodeSpeedLimit uint64    // Bytes/s, 0 = unlimited
	UserInfo       *sync.Map // key: "tag|email|uid" -> UserInfo (GetUserBucket 用 "tag|email|" 前缀匹配)
	BucketHub      *sync.Map // key: email -> *rate.Limiter (与 GetUserBucket/SetUserSpeed/LookupBucketByEmail 一致)
	UserOnlineIP   *sync.Map // key: email -> *emailIPMap (内层 ip -> *ipEntry + mu)
}

// KickCounter 累计每个 email 触发「连接数上限被拒绝」的次数(上报给主控算 delta → tg 通知)。
// 采用 sync.Map[email]*int64,累计语义(从 agent 启动开始单调递增);主控算 delta = current - prev_seen。
// (原为设备数「踢最旧」计数,现语义改为连接数超限拒绝次数。)
var KickCounter sync.Map // map[string]*int64

// connCount 每个 group 的**当前并发连接数**(group = "<user>|<物理父节点ID>",由主控下发)。
// 一个用户在同一物理节点(含其路由出站子账户)的所有 email 共享同一 group → 共享一份连接配额。
// AcquireConn 进连接 +1、ReleaseConn 出连接 -1;精确并发(靠 dispatcher 的 ctx AfterFunc 释放)。
var connCount sync.Map // map[string]*atomic.Int64

func connCounter(group string) *atomic.Int64 {
	if v, ok := connCount.Load(group); ok {
		return v.(*atomic.Int64)
	}
	v, _ := connCount.LoadOrStore(group, new(atomic.Int64))
	return v.(*atomic.Int64)
}

// lookupUserInfo 按 (tag,email) 前缀扫描找到该用户的限流配置。
func (l *Limiter) lookupUserInfo(tag, email string) (UserInfo, bool) {
	value, ok := l.InboundInfo.Load(tag)
	if !ok {
		return UserInfo{}, false
	}
	info := value.(*InboundInfo)
	var found UserInfo
	var hit bool
	expectedPrefix := fmt.Sprintf("%s|%s|", tag, email)
	info.UserInfo.Range(func(k, v interface{}) bool {
		key := k.(string)
		if len(key) >= len(expectedPrefix) && key[:len(expectedPrefix)] == expectedPrefix {
			found = v.(UserInfo)
			hit = true
			return false
		}
		return true
	})
	return found, hit
}

// AcquireConn 在新连接建立时调用:按 group 累计并发连接数并做「满额拒绝」判定。
//   - 返回 ok=false → 已达连接上限,dispatcher 应断流(不建出站);同时累计 KickCounter 供主控通知。
//   - 返回 ok=true  → 放行;调用方必须在连接结束时 ReleaseConn(group) 以精确 -1。
//
// connLimit<=0(不限)时仍计数(供用户视图展示当前连接数),但永不拒绝。
// group 为空(老主控未下发 ConnGroup)时退化按 email 计数,行为仍正确(只是不跨 email 共享)。
func (l *Limiter) AcquireConn(tag, email string) (ok bool, group string) {
	u, found := l.lookupUserInfo(tag, email)
	if !found {
		return true, "" // 无该用户限流记录(如未下发)→ 放行不计数
	}
	group = u.ConnGroup
	if group == "" {
		group = email
	}
	connLimit := u.DeviceLimit // 现语义 = 并发连接上限
	c := connCounter(group)
	n := c.Add(1)
	if connLimit > 0 && n > int64(connLimit) {
		c.Add(-1) // 撤销本次占用
		incrementKickCounter(email)
		return false, ""
	}
	return true, group
}

// ReleaseConn 在连接结束时调用,把 group 的并发连接数 -1(下限 0)。
func (l *Limiter) ReleaseConn(group string) {
	if group == "" {
		return
	}
	c := connCounter(group)
	if c.Add(-1) < 0 {
		c.Store(0)
	}
}

// ConnCountSnapshot 返回各 group 当前并发连接数(>0 的项),供上报主控/用户视图展示。
func (l *Limiter) ConnCountSnapshot() map[string]int64 {
	out := make(map[string]int64)
	connCount.Range(func(k, v interface{}) bool {
		if n := v.(*atomic.Int64).Load(); n > 0 {
			out[k.(string)] = n
		}
		return true
	})
	return out
}

type Limiter struct {
	InboundInfo *sync.Map // key: tag -> *InboundInfo
	// autoLimited 标记"当前正被自动限速(SetUserSpeed 临时覆盖)的 email"。
	// GetOnlineUsers 的空闲 bucket 清理 与 UpdateInboundLimiter 的 limit==0 处理都必须跳过这些 email,
	// 否则会把 auto 限速正在用的同一个 bucket 删掉/重置,导致新连接 GetUserBucket 新建无限 bucket → 限速形同虚设。
	autoLimited sync.Map // key: email -> struct{}
}

func New() *Limiter {
	return &Limiter{
		InboundInfo: new(sync.Map),
	}
}

func (l *Limiter) AddInboundLimiter(tag string, nodeSpeedLimit uint64, users []UserInfo) {
	info := &InboundInfo{
		Tag:            tag,
		NodeSpeedLimit: nodeSpeedLimit,
		UserInfo:       new(sync.Map),
		BucketHub:      new(sync.Map),
		UserOnlineIP:   new(sync.Map),
	}
	for _, u := range users {
		key := fmt.Sprintf("%s|%s|%d", tag, u.Email, u.UID)
		info.UserInfo.Store(key, u)
	}
	l.InboundInfo.Store(tag, info)
}

// SyncInboundLimiter 用主控下发的最新配置整体刷新一个 inbound 的限速。
//
// 与 AddInboundLimiter 的关键区别:**沿用原有的 BucketHub 和 UserOnlineIP**。
//
// 为什么必须沿用:dispatcher 只在连接建立时调一次 GetUserBucket,把拿到的
// *rate.Limiter 包进 RateWriter 伴随该连接终生(见 dispatcher/default.go)。
// 换掉 BucketHub 之后,存量连接手里仍是旧桶对象 —— 改了限速对已经连上的人
// 完全不生效,只有新建连接才拿到新值。线上表现就是"配置后过一阵才生效、
// 客户端重连一下就生效了"。
//
// UserOnlineIP 同理:重建会把在线设备统计清零,而限速下发在每次 WS 重连时
// 都会发生,等于设备数统计被反复清空。
func (l *Limiter) SyncInboundLimiter(tag string, nodeSpeedLimit uint64, users []UserInfo) {
	old, ok := l.InboundInfo.Load(tag)
	if !ok {
		l.AddInboundLimiter(tag, nodeSpeedLimit, users)
		return
	}
	prev := old.(*InboundInfo)

	// 仍然整体换 InboundInfo 而不是原地改 NodeSpeedLimit:后者会与 GetUserBucket
	// 的无锁读构成数据竞争。换指针由 sync.Map.Store 保证可见性。
	info := &InboundInfo{
		Tag:            tag,
		NodeSpeedLimit: nodeSpeedLimit,
		UserInfo:       new(sync.Map),
		BucketHub:      prev.BucketHub,
		UserOnlineIP:   prev.UserOnlineIP,
	}
	for _, u := range users {
		info.UserInfo.Store(fmt.Sprintf("%s|%s|%d", tag, u.Email, u.UID), u)
	}
	l.InboundInfo.Store(tag, info)

	// 把新速率写进存量桶,存量连接立刻感知。
	// limit==0(无限制)不动:同 UpdateInboundLimiter 的说明 —— 删桶或原地置无限
	// 都会破坏正在生效的自动限速。空闲桶由 GetOnlineUsers 统一回收。
	for _, u := range users {
		limit := determineRate(nodeSpeedLimit, u.SpeedLimit)
		if limit <= 0 {
			continue
		}
		if bucket, ok := info.BucketHub.Load(u.Email); ok {
			b := bucket.(*rate.Limiter)
			b.SetLimit(rate.Limit(limit))
			b.SetBurst(calcBurst(limit))
		}
	}
}

func (l *Limiter) UpdateInboundLimiter(tag string, users []UserInfo) {
	value, ok := l.InboundInfo.Load(tag)
	if !ok {
		l.AddInboundLimiter(tag, 0, users)
		return
	}
	info := value.(*InboundInfo)
	for _, u := range users {
		key := fmt.Sprintf("%s|%s|%d", tag, u.Email, u.UID)
		info.UserInfo.Store(key, u)
		limit := determineRate(info.NodeSpeedLimit, u.SpeedLimit)
		// BucketHub 以 email 为 key(与 GetUserBucket 存的一致),不能用组合 key,否则 Load 永远 miss。
		if limit > 0 {
			if bucket, ok := info.BucketHub.Load(u.Email); ok {
				limiter := bucket.(*rate.Limiter)
				limiter.SetLimit(rate.Limit(limit))
				limiter.SetBurst(calcBurst(limit))
			}
		}
		// 静态限速为 0(无限制):**什么都不做**。
		// 既不能 Delete(b3b803a 的回归:删掉后存量连接持有的旧 bucket 与 SetUserSpeed 新建的 bucket 脱钩,
		// 自动限速对存量连接失效),也不能原地置无限(会撤销正在生效的 auto 限速)。
		// 空闲 bucket 统一由 GetOnlineUsers 清理,且那里会跳过正被 auto 限速的 email。
	}
}

func (l *Limiter) DeleteInboundLimiter(tag string) {
	l.InboundInfo.Delete(tag)
}

func (l *Limiter) GetUserBucket(tag string, email string, ip string) (limiter *rate.Limiter, hasLimit bool, reject bool) {
	value, ok := l.InboundInfo.Load(tag)
	if !ok {
		return nil, false, false
	}

	info := value.(*InboundInfo)
	nodeLimit := info.NodeSpeedLimit

	var userLimit uint64
	var uid int

	// Find user info by scanning keys with matching tag and email prefix
	info.UserInfo.Range(func(k, v interface{}) bool {
		key := k.(string)
		u := v.(UserInfo)
		expectedPrefix := fmt.Sprintf("%s|%s|", tag, email)
		if len(key) >= len(expectedPrefix) && key[:len(expectedPrefix)] == expectedPrefix {
			uid = u.UID
			userLimit = u.SpeedLimit
			return false
		}
		return true
	})

	// 连接数限制已迁到 AcquireConn(按 group 精确并发计数、满额拒绝),与此处解耦。
	// 这里只记录 IP 给在线用户上报用(GetOnlineUsers 每周期重置)。reject 恒为 false。
	now := time.Now()
	ipMap := newEmailIPMap()
	actual, _ := info.UserOnlineIP.LoadOrStore(email, ipMap)
	em := actual.(*emailIPMap)
	em.mu.Lock()
	em.m[ip] = &ipEntry{uid: uid, lastSeen: now}
	em.mu.Unlock()

	// Speed limit
	limit := determineRate(nodeLimit, userLimit)
	if limit > 0 {
		newLimiter := rate.NewLimiter(rate.Limit(limit), calcBurst(limit))
		if v, loaded := info.BucketHub.LoadOrStore(email, newLimiter); loaded {
			return v.(*rate.Limiter), true, false
		}
		return newLimiter, true, false
	}

	// No static limit — create an unlimited bucket so auto speed limit can
	// dynamically throttle existing connections via SetUserSpeed.
	unlimited := rate.NewLimiter(rate.Limit(math.MaxFloat64), math.MaxInt)
	if v, loaded := info.BucketHub.LoadOrStore(email, unlimited); loaded {
		return v.(*rate.Limiter), true, false
	}
	return unlimited, true, false
}

func (l *Limiter) RateWriter(writer buf.Writer, limiter *rate.Limiter) buf.Writer {
	return NewRateWriter(writer, limiter)
}

// GetOnlineUsers returns email -> []ip mapping for the given inbound tag.
// It also resets the online IP tracking for the next collection cycle.
func (l *Limiter) GetOnlineUsers(tag string) map[string][]string {
	value, ok := l.InboundInfo.Load(tag)
	if !ok {
		return nil
	}
	info := value.(*InboundInfo)
	result := make(map[string][]string)

	// Clean up stale buckets(本周期没有活跃 IP 的 email)。
	// 跳过正被 auto 限速的 email:它的 bucket 正被 SetUserSpeed 临时限速,删了之后新连接 GetUserBucket
	// 会新建无限 bucket → auto 限速对新连接/重连失效(限速看起来"没用")。
	info.BucketHub.Range(func(key, _ interface{}) bool {
		email := key.(string)
		if _, exists := info.UserOnlineIP.Load(email); exists {
			return true
		}
		if _, auto := l.autoLimited.Load(email); auto {
			return true
		}
		info.BucketHub.Delete(email)
		return true
	})

	info.UserOnlineIP.Range(func(key, value interface{}) bool {
		email := key.(string)
		em := value.(*emailIPMap)
		var ips []string
		em.mu.Lock()
		for ip := range em.m {
			ips = append(ips, ip)
		}
		em.mu.Unlock()
		if len(ips) > 0 {
			result[email] = ips
		}
		info.UserOnlineIP.Delete(email)
		return true
	})

	return result
}

// incrementKickCounter 每被"踢最旧"一次,该 email 累计 +1。Phase 3B 主控收集 delta。
func incrementKickCounter(email string) {
	var counter int64 = 1
	if v, loaded := KickCounter.LoadOrStore(email, &counter); loaded {
		atomic.AddInt64(v.(*int64), 1)
	}
}

// SnapshotKickCounter 返回当前所有 email 的累计被踢次数(给上报用),不清零。
// 主控按 delta = current - last_seen_per_email 算单周期增量。
func SnapshotKickCounter() map[string]int64 {
	out := make(map[string]int64)
	KickCounter.Range(func(k, v interface{}) bool {
		email := k.(string)
		out[email] = atomic.LoadInt64(v.(*int64))
		return true
	})
	return out
}

// SetUserSpeed temporarily overrides a user's speed limit bucket.
// When speedLimit=0, restores the user's original rate (static limit or unlimited).
func (l *Limiter) SetUserSpeed(tag, email string, speedLimit uint64) {
	value, ok := l.InboundInfo.Load(tag)
	if !ok {
		return
	}
	info := value.(*InboundInfo)
	if speedLimit > 0 {
		// 标记该 email 正被 auto 限速,GetOnlineUsers/UpdateInboundLimiter 清理时跳过,防止 bucket 被删/重置。
		l.autoLimited.Store(email, struct{}{})
		if v, ok := info.BucketHub.Load(email); ok {
			lim := v.(*rate.Limiter)
			lim.SetLimit(rate.Limit(speedLimit))
			lim.SetBurst(calcBurst(speedLimit))
		} else {
			info.BucketHub.Store(email, rate.NewLimiter(rate.Limit(speedLimit), calcBurst(speedLimit)))
		}
	} else {
		// 解除 auto 限速标记,后续空闲时允许 GetOnlineUsers 清理。
		l.autoLimited.Delete(email)
		// Restore to original rate — modify the existing bucket in place
		// so existing connections (holding a reference) are also restored.
		origLimit := l.getUserStaticLimit(info, tag, email)
		if v, ok := info.BucketHub.Load(email); ok {
			lim := v.(*rate.Limiter)
			if origLimit > 0 {
				lim.SetLimit(rate.Limit(origLimit))
				lim.SetBurst(calcBurst(origLimit))
			} else {
				lim.SetLimit(rate.Limit(math.MaxFloat64))
				lim.SetBurst(math.MaxInt)
			}
		}
	}
}

func (l *Limiter) getUserStaticLimit(info *InboundInfo, tag, email string) uint64 {
	var userLimit uint64
	expectedPrefix := fmt.Sprintf("%s|%s|", tag, email)
	info.UserInfo.Range(func(k, v interface{}) bool {
		key := k.(string)
		if len(key) >= len(expectedPrefix) && key[:len(expectedPrefix)] == expectedPrefix {
			u := v.(UserInfo)
			userLimit = u.SpeedLimit
			return false
		}
		return true
	})
	return determineRate(info.NodeSpeedLimit, userLimit)
}

func calcBurst(bytesPerSec uint64) int {
	b := bytesPerSec / 4 // 250ms worth
	if b < 64<<10 {
		return 64 << 10 // min 64KB
	}
	if b > 256<<10 {
		return 256 << 10 // max 256KB
	}
	return int(b)
}

// determineRate returns the minimum non-zero rate between node and user limits.
func determineRate(nodeLimit, userLimit uint64) uint64 {
	if nodeLimit == 0 && userLimit == 0 {
		return 0
	}
	if nodeLimit == 0 {
		return userLimit
	}
	if userLimit == 0 {
		return nodeLimit
	}
	if nodeLimit < userLimit {
		return nodeLimit
	}
	return userLimit
}
