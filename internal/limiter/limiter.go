package limiter

import (
	"fmt"
	"math"
	"sync"

	"github.com/xtls/xray-core/common/buf"
	"golang.org/x/time/rate"
)

type InboundInfo struct {
	Tag            string
	NodeSpeedLimit uint64    // Bytes/s, 0 = unlimited
	UserInfo       *sync.Map // key: "tag|email" -> UserInfo
	BucketHub      *sync.Map // key: "tag|email" -> *rate.Limiter
	UserOnlineIP   *sync.Map // key: "tag|email" -> *sync.Map{ip -> uid}
}

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
		if limit > 0 {
			if bucket, ok := info.BucketHub.Load(key); ok {
				limiter := bucket.(*rate.Limiter)
				limiter.SetLimit(rate.Limit(limit))
				limiter.SetBurst(calcBurst(limit))
			}
		} else {
			info.BucketHub.Delete(key)
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

	// Device limit check
	if deviceLimit > 0 {
		ipMap := new(sync.Map)
		ipMap.Store(ip, uid)
		if v, loaded := info.UserOnlineIP.LoadOrStore(email, ipMap); loaded {
			existingMap := v.(*sync.Map)
			if _, exists := existingMap.LoadOrStore(ip, uid); !exists {
				count := 0
				existingMap.Range(func(_, _ interface{}) bool {
					count++
					return true
				})
				if count > deviceLimit {
					existingMap.Delete(ip)
					return nil, false, true
				}
			}
		}
	} else {
		// Still track IP for online user reporting
		ipMap := new(sync.Map)
		ipMap.Store(ip, uid)
		if v, loaded := info.UserOnlineIP.LoadOrStore(email, ipMap); loaded {
			existingMap := v.(*sync.Map)
			existingMap.Store(ip, uid)
		}
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
		ipMap := value.(*sync.Map)
		var ips []string
		ipMap.Range(func(k, _ interface{}) bool {
			ips = append(ips, k.(string))
			return true
		})
		if len(ips) > 0 {
			result[email] = ips
		}
		info.UserOnlineIP.Delete(email)
		return true
	})

	return result
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
