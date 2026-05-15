package embedded

import (
	"context"
	"log"
	"sync"

	"github.com/xtls/xray-core/app/proxyman/command"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/core"
	feature_inbound "github.com/xtls/xray-core/features/inbound"
	feature_outbound "github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/stats"

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

func (e *EmbeddedXray) Start() error {
	pbConfig, err := buildCoreConfig(e.configPath)
	if err != nil {
		return err
	}

	instance, err := core.New(pbConfig)
	if err != nil {
		return err
	}

	if err := instance.Start(); err != nil {
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

	log.Printf("[EmbeddedXray] Started successfully")
	return nil
}

func (e *EmbeddedXray) Stop() error {
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
	return e.Start()
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

