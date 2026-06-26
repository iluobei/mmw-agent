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

// KickCounter 累计每个 email 被「踢最旧」的次数(Phase 3B 上报给主控用,主控收到 delta → tg 通知)
// 采用 sync.Map[email]*int64,累计语义(从 agent 启动开始单调递增);主控算 delta = current - prev_seen
var KickCounter sync.Map // map[string]*int64

type Limiter struct {
	InboundInfo *sync.Map // key: tag -> *InboundInfo
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
		// BucketHub 以 email 为 key(与 GetUserBucket 存的一致),不能用组合 key,否则 Load/Delete 永远 miss。
		if limit > 0 {
			if bucket, ok := info.BucketHub.Load(u.Email); ok {
				limiter := bucket.(*rate.Limiter)
				limiter.SetLimit(rate.Limit(limit))
				limiter.SetBurst(calcBurst(limit))
			}
		} else {
			info.BucketHub.Delete(u.Email)
		}
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
	var deviceLimit, uid int

	// Find user info by scanning keys with matching tag and email prefix
	info.UserInfo.Range(func(k, v interface{}) bool {
		key := k.(string)
		u := v.(UserInfo)
		expectedPrefix := fmt.Sprintf("%s|%s|", tag, email)
		if len(key) >= len(expectedPrefix) && key[:len(expectedPrefix)] == expectedPrefix {
			uid = u.UID
			userLimit = u.SpeedLimit
			deviceLimit = u.DeviceLimit
			return false
		}
		return true
	})

	// Device limit check — 策略改为「踢最旧」(LRU evict):
	//   - 已有该 ip → 仅更新 lastSeen,放行
	//   - 新 ip 且 count < limit → 加进去,放行
	//   - 新 ip 且 count >= limit → 找 lastSeen 最早的删,加新的进去,KickCounter[email]++,放行
	//
	// 注:依赖客户端连接 keepalive 超时让旧连接自然断;agent 无主动断 xray 连接的 API。
	// 被踢的 IP 下次包到达时走"新 IP 加入"路径,如果还在超限会再次被踢出。
	now := time.Now()
	if deviceLimit > 0 {
		ipMap := newEmailIPMap()
		actual, _ := info.UserOnlineIP.LoadOrStore(email, ipMap)
		em := actual.(*emailIPMap)
		em.mu.Lock()
		if entry, exists := em.m[ip]; exists {
			entry.lastSeen = now
		} else if len(em.m) < deviceLimit {
			em.m[ip] = &ipEntry{uid: uid, lastSeen: now}
		} else {
			// evict 最旧:扫一遍找 minLastSeen
			var oldestIP string
			var oldestTime time.Time
			first := true
			for k, v := range em.m {
				if first || v.lastSeen.Before(oldestTime) {
					oldestIP = k
					oldestTime = v.lastSeen
					first = false
				}
			}
			if oldestIP != "" {
				delete(em.m, oldestIP)
			}
			em.m[ip] = &ipEntry{uid: uid, lastSeen: now}
			// 累计 kick 次数(Phase 3B 上报用)
			incrementKickCounter(email)
		}
		em.mu.Unlock()
	} else {
		// 无 device limit,仍要记 IP 给 online users 上报
		ipMap := newEmailIPMap()
		actual, _ := info.UserOnlineIP.LoadOrStore(email, ipMap)
		em := actual.(*emailIPMap)
		em.mu.Lock()
		em.m[ip] = &ipEntry{uid: uid, lastSeen: now}
		em.mu.Unlock()
	}

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

	// Clean up stale buckets
	info.BucketHub.Range(func(key, _ interface{}) bool {
		email := key.(string)
		if _, exists := info.UserOnlineIP.Load(email); !exists {
			info.BucketHub.Delete(email)
		}
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
		if v, ok := info.BucketHub.Load(email); ok {
			lim := v.(*rate.Limiter)
			lim.SetLimit(rate.Limit(speedLimit))
			lim.SetBurst(calcBurst(speedLimit))
		} else {
			info.BucketHub.Store(email, rate.NewLimiter(rate.Limit(speedLimit), calcBurst(speedLimit)))
		}
	} else {
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
