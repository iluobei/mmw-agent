package embedded

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/xtls/xray-core/app/proxyman/command"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/core"
	feature_inbound "github.com/xtls/xray-core/features/inbound"
	feature_outbound "github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/stats"
	xproxy "github.com/xtls/xray-core/proxy"

	mydispatcher "mmw-agent/internal/dispatcher"
	"mmw-agent/internal/limiter"
)

type EmbeddedXray struct {
	configPath   string
	instance     *core.Instance
	dispatcher   *mydispatcher.Dispatcher
	statsManager stats.Manager
	speedMonitor *SpeedMonitor
	mu           sync.RWMutex
}

func New(configPath string) *EmbeddedXray {
	return &EmbeddedXray{
		configPath:   configPath,
		speedMonitor: NewSpeedMonitor(),
	}
}

func (e *EmbeddedXray) GetSpeedMonitor() *SpeedMonitor {
	return e.speedMonitor
}

func (e *EmbeddedXray) Start() (retErr error) {
	// xray-core 内部偶发 panic(端口冲突 / 配置异常等)。没有 recover 时整个 agent 进程被带崩,
	// 主控看到 "connection reset by peer";加 recover 后返回 error,handler 正常回 500。
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("xray start panicked: %v", r)
		}
	}()
	pbConfig, err := buildCoreConfig(e.configPath)
	if err != nil {
		return err
	}

	instance, err := e.safeNewInstance(pbConfig)
	if err != nil {
		return err
	}

	if err := e.safeInstanceStart(instance); err != nil {
		instance.Close()
		return err
	}

	e.mu.Lock()
	e.instance = instance
	e.statsManager, _ = instance.GetFeature(stats.ManagerType()).(stats.Manager)

	// Get our custom dispatcher for limiter access
	if d := instance.GetFeature(mydispatcher.Type()); d != nil {
		e.dispatcher, _ = d.(*mydispatcher.Dispatcher)
	}
	e.mu.Unlock()

	// 注册 vision splice 后的 conn-wrap 钩子,让 xtls-rprx-vision 节点的限速也能生效。
	// hook 闭包持有 EmbeddedXray 引用,每次 splice 触发时按 email 查 per-user rate.Limiter;
	// 拿不到 limiter (limit=0 或用户已被踢) 时返回 nil,vision 走原零拷贝路径,无开销。
	// 重启 mmw-agent 时 instance 重新初始化,旧 limiter 引用会被新 hook 覆盖。
	xproxy.SetVisionLimiterHook(func(email string, rawConn xnet.Conn) xnet.Conn {
		l := e.GetLimiter()
		if l == nil {
			log.Printf("[VisionLimiter] %s: skip (limiter not ready)", email)
			return nil
		}
		bucket := l.LookupBucketByEmail(email)
		if bucket == nil {
			log.Printf("[VisionLimiter] %s: skip (no bucket / unlimited)", email)
			return nil
		}
		log.Printf("[VisionLimiter] %s: wrapping rawConn with bucket rate=%v burst=%d", email, bucket.Limit(), bucket.Burst())
		return limiter.NewRateLimitedConn(rawConn, bucket)
	})

	log.Printf("[EmbeddedXray] Started successfully (vision limiter hook registered)")
	return nil
}

func (e *EmbeddedXray) safeNewInstance(pbConfig *core.Config) (inst *core.Instance, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("xray core.New panicked: %v", r)
		}
	}()
	inst, err = core.New(pbConfig)
	return
}

// safeInstanceStart 把 instance.Start 包在 recover 里 — xray-core 启动期 panic 不再带崩 agent 进程。
func (e *EmbeddedXray) safeInstanceStart(inst *core.Instance) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("xray instance.Start panicked: %v", r)
		}
	}()
	return inst.Start()
}

func (e *EmbeddedXray) Stop() (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("xray stop panicked: %v", r)
		}
	}()
	e.mu.Lock()
	instance := e.instance
	e.instance = nil
	e.dispatcher = nil
	e.statsManager = nil
	e.mu.Unlock()

	if instance != nil {
		return instance.Close()
	}
	return nil
}

func (e *EmbeddedXray) Restart() error {
	log.Printf("[EmbeddedXray] Restarting...")
	if err := e.Stop(); err != nil {
		log.Printf("[EmbeddedXray] Stop error: %v", err)
	}
	// Wait for OS to release listener ports (metrics, gRPC API)
	time.Sleep(500 * time.Millisecond)

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		lastErr = e.Start()
		if lastErr == nil {
			return nil
		}
		log.Printf("[EmbeddedXray] Start attempt %d failed: %v", attempt, lastErr)
		if attempt < 3 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
	}
	return lastErr
}

func (e *EmbeddedXray) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.instance != nil
}

func (e *EmbeddedXray) GetLimiter() *limiter.Limiter {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.dispatcher != nil {
		return e.dispatcher.Limiter
	}
	return nil
}

func (e *EmbeddedXray) UpdateLimiter(tag string, users []limiter.UserInfo) {
	l := e.GetLimiter()
	if l == nil {
		return
	}
	l.UpdateInboundLimiter(tag, users)
}

func (e *EmbeddedXray) GetOnlineUsers(tag string) map[string][]string {
	l := e.GetLimiter()
	if l == nil {
		return nil
	}
	return l.GetOnlineUsers(tag)
}

// AddUser adds a user to an inbound handler.
func (e *EmbeddedXray) AddUser(inboundTag string, user *protocol.User) error {
	e.mu.RLock()
	instance := e.instance
	e.mu.RUnlock()
	if instance == nil {
		return errNotRunning
	}

	ibm := instance.GetFeature(feature_inbound.ManagerType()).(feature_inbound.Manager)
	ctx := context.Background()
	handler, err := ibm.GetHandler(ctx, inboundTag)
	if err != nil {
		return err
	}

	op := &command.AddUserOperation{User: user}
	return op.ApplyInbound(ctx, handler)
}

// RemoveUser removes a user from an inbound handler.
func (e *EmbeddedXray) RemoveUser(inboundTag string, email string) error {
	e.mu.RLock()
	instance := e.instance
	e.mu.RUnlock()
	if instance == nil {
		return errNotRunning
	}

	ibm := instance.GetFeature(feature_inbound.ManagerType()).(feature_inbound.Manager)
	ctx := context.Background()
	handler, err := ibm.GetHandler(ctx, inboundTag)
	if err != nil {
		return err
	}

	op := &command.RemoveUserOperation{Email: email}
	return op.ApplyInbound(ctx, handler)
}

// AddInbound adds a new inbound handler from a core.InboundHandlerConfig.
func (e *EmbeddedXray) AddInbound(config *core.InboundHandlerConfig) error {
	e.mu.RLock()
	instance := e.instance
	e.mu.RUnlock()
	if instance == nil {
		return errNotRunning
	}

	ibm := instance.GetFeature(feature_inbound.ManagerType()).(feature_inbound.Manager)
	rawHandler, err := core.CreateObject(instance, config)
	if err != nil {
		return err
	}
	handler, ok := rawHandler.(feature_inbound.Handler)
	if !ok {
		return errInvalidHandler
	}
	return ibm.AddHandler(context.Background(), handler)
}

// RemoveInbound removes an inbound handler by tag.
func (e *EmbeddedXray) RemoveInbound(tag string) error {
	e.mu.RLock()
	instance := e.instance
	e.mu.RUnlock()
	if instance == nil {
		return errNotRunning
	}

	ibm := instance.GetFeature(feature_inbound.ManagerType()).(feature_inbound.Manager)
	return ibm.RemoveHandler(context.Background(), tag)
}

// ListInbounds returns all inbound handler tags.
func (e *EmbeddedXray) ListInbounds() []string {
	e.mu.RLock()
	instance := e.instance
	e.mu.RUnlock()
	if instance == nil {
		return nil
	}

	ibm := instance.GetFeature(feature_inbound.ManagerType()).(feature_inbound.Manager)
	handlers := ibm.ListHandlers(context.Background())
	tags := make([]string, 0, len(handlers))
	for _, h := range handlers {
		tags = append(tags, h.Tag())
	}
	return tags
}

// AddOutbound adds a new outbound handler.
func (e *EmbeddedXray) AddOutbound(config *core.OutboundHandlerConfig) error {
	e.mu.RLock()
	instance := e.instance
	e.mu.RUnlock()
	if instance == nil {
		return errNotRunning
	}

	obm := instance.GetFeature(feature_outbound.ManagerType()).(feature_outbound.Manager)
	rawHandler, err := core.CreateObject(instance, config)
	if err != nil {
		return err
	}
	handler, ok := rawHandler.(feature_outbound.Handler)
	if !ok {
		return errInvalidHandler
	}
	return obm.AddHandler(context.Background(), handler)
}

// RemoveOutbound removes an outbound handler by tag.
func (e *EmbeddedXray) RemoveOutbound(tag string) error {
	e.mu.RLock()
	instance := e.instance
	e.mu.RUnlock()
	if instance == nil {
		return errNotRunning
	}

	obm := instance.GetFeature(feature_outbound.ManagerType()).(feature_outbound.Manager)
	return obm.RemoveHandler(context.Background(), tag)
}

// GetTraffic returns a counter value by name pattern (e.g. "user>>>email>>>traffic>>>uplink").
func (e *EmbeddedXray) GetTraffic(name string) int64 {
	e.mu.RLock()
	sm := e.statsManager
	e.mu.RUnlock()
	if sm == nil {
		return 0
	}
	c := sm.GetCounter(name)
	if c == nil {
		return 0
	}
	return c.Value()
}

var (
	errNotRunning    = &EmbeddedError{"xray instance not running"}
	errInvalidHandler = &EmbeddedError{"created object is not a valid handler"}
)

type EmbeddedError struct {
	msg string
}

func (e *EmbeddedError) Error() string { return e.msg }

