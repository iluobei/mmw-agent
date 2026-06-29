package limiter

import (
	"testing"

	"golang.org/x/time/rate"
)

// 验证 B2:UpdateInboundLimiter 改限速后,GetUserBucket 已创建的同一 bucket 的 Limit 被更新。
// 修复前 BucketHub 用组合 key(tag|email|uid)而 GetUserBucket 用 email,导致 Load miss、SetLimit 不执行。
func TestUpdateInboundLimiterUpdatesExistingBucket(t *testing.T) {
	l := New()
	const tag = "ss-test"
	const email = "u@example.com"

	l.AddInboundLimiter(tag, 0, []UserInfo{{UID: 1, Email: email, SpeedLimit: 1 << 20}}) // 1MB/s

	// 连接建立 → 创建 bucket
	bucket, hasLimit, reject := l.GetUserBucket(tag, email, "1.2.3.4")
	if reject || !hasLimit || bucket == nil {
		t.Fatalf("GetUserBucket: reject=%v hasLimit=%v bucket=%v", reject, hasLimit, bucket)
	}
	if int(bucket.Limit()) != 1<<20 {
		t.Fatalf("初始 limit 应为 1MiB/s, got %v", bucket.Limit())
	}

	// 主控下发新限速 2MB/s
	l.UpdateInboundLimiter(tag, []UserInfo{{UID: 1, Email: email, SpeedLimit: 2 << 20}})

	// 同一 bucket(现有连接持有的引用)的 limit 应被就地更新到 2MB/s
	if int(bucket.Limit()) != 2<<20 {
		t.Fatalf("UpdateInboundLimiter 未命中 bucket(key 不一致回归): limit 仍为 %v, 期望 %d", bucket.Limit(), 2<<20)
	}
}

// 限速降到 0(取消静态限速)时,UpdateInboundLimiter 必须【保留同一 bucket 对象并原地置为无限】,
// 而不是删除它。删除会让存量连接(RateWriter 持有旧 bucket 引用)在随后的自动限速 SetUserSpeed 时
// 拿不到同一对象 —— SetUserSpeed 新建的 bucket 对存量连接无效,导致"限速对已有连接不生效"。
// 这是 b3b803a 把 BucketHub key 从组合 key 改成 email key 后,Delete 真正生效引入的回归。
func TestUpdateInboundLimiterZeroKeepsBucketForLiveThrottle(t *testing.T) {
	l := New()
	const tag = "ss-test"
	const email = "u@example.com"

	l.AddInboundLimiter(tag, 0, []UserInfo{{UID: 1, Email: email, SpeedLimit: 1 << 20}})
	// 模拟连接建立,拿到 bucket 引用(dispatcher 的 RateWriter 会一直持有它)
	bucket, _, _ := l.GetUserBucket(tag, email, "1.2.3.4")
	if bucket == nil {
		t.Fatal("前置:bucket 应已创建")
	}

	value, _ := l.InboundInfo.Load(tag)
	info := value.(*InboundInfo)

	// 主控下发 SpeedLimit=0(取消静态限速)
	l.UpdateInboundLimiter(tag, []UserInfo{{UID: 1, Email: email, SpeedLimit: 0}})

	// bucket 仍在 hub,且是同一个对象、被原地置为无限(而非删除/新建)
	v, ok := info.BucketHub.Load(email)
	if !ok {
		t.Fatal("SpeedLimit=0 后 bucket 不应被删除(删了会让 SetUserSpeed 对存量连接失效)")
	}
	if v.(*rate.Limiter) != bucket {
		t.Fatal("应原地复用同一 bucket 对象,而不是新建")
	}

	// 关键:随后自动限速 SetUserSpeed 必须作用到【存量连接持有的同一 bucket】
	l.SetUserSpeed(tag, email, 512<<10) // 512KB/s
	if int(bucket.Limit()) != 512<<10 {
		t.Fatalf("SetUserSpeed 未作用到存量连接的 bucket: limit=%v 期望 %d", bucket.Limit(), 512<<10)
	}
}

func bucketOf(l *Limiter, tag, email string) (*rate.Limiter, bool) {
	v, ok := l.InboundInfo.Load(tag)
	if !ok {
		return nil, false
	}
	b, ok := v.(*InboundInfo).BucketHub.Load(email)
	if !ok {
		return nil, false
	}
	return b.(*rate.Limiter), true
}

// auto 限速(SetUserSpeed)生效后,空闲 bucket 清理(GetOnlineUsers)与静态0的 user sync(UpdateInboundLimiter)
// 都不能删/撤销该 email 的 bucket —— 否则新连接 GetUserBucket 会新建无限 bucket,auto 限速对新连接/重连失效
// (表现为"限速一会儿就没用了")。restore 后标记解除,空闲 bucket 才允许回收。
func TestAutoLimitSurvivesCleanupAndSync(t *testing.T) {
	l := New()
	const tag = "ss-test"
	const email = "u@example.com"
	l.AddInboundLimiter(tag, 0, []UserInfo{{UID: 1, Email: email, SpeedLimit: 0}}) // 无静态限速

	bucket, _, _ := l.GetUserBucket(tag, email, "1.2.3.4") // 建无限 bucket
	l.SetUserSpeed(tag, email, 250000)                     // auto 限速 2Mbps=250000B/s
	if int(bucket.Limit()) != 250000 {
		t.Fatalf("auto 限速未生效: %v", bucket.Limit())
	}

	// 模拟多个上报周期:存量长连接不再调 GetUserBucket → UserOnlineIP 被 drain,email 变"空闲"
	l.GetOnlineUsers(tag)
	l.GetOnlineUsers(tag)
	// 静态0 的 user sync
	l.UpdateInboundLimiter(tag, []UserInfo{{UID: 1, Email: email, SpeedLimit: 0}})

	b, ok := bucketOf(l, tag, email)
	if !ok {
		t.Fatal("auto 限速期间 bucket 被清理(新连接会绕过限速)")
	}
	if b != bucket {
		t.Fatal("bucket 对象被替换,存量连接与新连接脱钩")
	}
	if int(bucket.Limit()) != 250000 {
		t.Fatalf("auto 限速被撤销: limit=%v", bucket.Limit())
	}

	// restore 后,标记解除,空闲 bucket 应被回收
	l.SetUserSpeed(tag, email, 0)
	l.GetOnlineUsers(tag)
	l.GetOnlineUsers(tag)
	if _, ok := bucketOf(l, tag, email); ok {
		t.Fatal("restore 后空闲 bucket 应被清理")
	}
}
