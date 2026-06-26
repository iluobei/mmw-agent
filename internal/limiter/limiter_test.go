package limiter

import "testing"

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

// 限速降到 0(禁用静态限速)时,UpdateInboundLimiter 应能用 email key 删除 bucket。
func TestUpdateInboundLimiterDeletesBucketOnZero(t *testing.T) {
	l := New()
	const tag = "ss-test"
	const email = "u@example.com"

	l.AddInboundLimiter(tag, 0, []UserInfo{{UID: 1, Email: email, SpeedLimit: 1 << 20}})
	_, _, _ = l.GetUserBucket(tag, email, "1.2.3.4") // 建立 bucket

	value, _ := l.InboundInfo.Load(tag)
	info := value.(*InboundInfo)
	if _, ok := info.BucketHub.Load(email); !ok {
		t.Fatal("前置:bucket 应已存在")
	}

	l.UpdateInboundLimiter(tag, []UserInfo{{UID: 1, Email: email, SpeedLimit: 0}})
	if _, ok := info.BucketHub.Load(email); ok {
		t.Fatal("SpeedLimit=0 后 bucket 应被删除(email key)")
	}
}
