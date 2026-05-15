package embedded

import (
	"log"
	"sync"
	"time"

	"mmw-agent/internal/limiter"
)

type AutoSpeedLimitRule struct {
	Type             string  `json:"type"`               // "sustained" | "burst"
	ThresholdMbps    float64 `json:"threshold_mbps"`     // 触发阈值 (Mbps)
	SustainedSeconds int     `json:"sustained_seconds"`  // sustained: 持续时长; burst: 单次最短时长
	WindowSeconds    int     `json:"window_seconds"`     // burst: 时间窗口
	BurstCount       int     `json:"burst_count"`        // burst: 窗口内触发次数
	LimitMbps        float64 `json:"limit_mbps"`         // 限速后速率 (Mbps)
	LimitDuration    int     `json:"limit_duration"`     // 限速持续时间 (秒)
}

type userSpeedState struct {
	sustainedStart time.Time
	burstEvents    []time.Time
	limitedUntil   time.Time
}

type SpeedMonitor struct {
	mu         sync.Mutex
	rules      []AutoSpeedLimitRule
	userState  map[string]*userSpeedState
	userSpeeds map[string]int64 // email → Bytes/s
	limiter    *limiter.Limiter
}

func NewSpeedMonitor() *SpeedMonitor {
	return &SpeedMonitor{
		userState:  make(map[string]*userSpeedState),
		userSpeeds: make(map[string]int64),
	}
}

func (m *SpeedMonitor) SetLimiter(l *limiter.Limiter) {
	m.mu.Lock()
	m.limiter = l
	m.mu.Unlock()
}

func (m *SpeedMonitor) UpdateRules(rules []AutoSpeedLimitRule) {
	m.mu.Lock()
	m.rules = rules
	m.mu.Unlock()
}

func (m *SpeedMonitor) GetUserSpeeds() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]int64, len(m.userSpeeds))
	for k, v := range m.userSpeeds {
		result[k] = v
	}
	return result
}

func (m *SpeedMonitor) Evaluate(userDeltas map[string]int64, elapsed time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if elapsed <= 0 {
		return
	}

	now := time.Now()
	secs := elapsed.Seconds()

	for email := range m.userSpeeds {
		delete(m.userSpeeds, email)
	}

	for email, delta := range userDeltas {
		speed := int64(float64(delta) / secs)
		if speed < 0 {
			speed = 0
		}
		m.userSpeeds[email] = speed
	}

	if len(m.rules) == 0 || m.limiter == nil {
		return
	}

	for email, speed := range m.userSpeeds {
		state := m.userState[email]
		if state == nil {
			state = &userSpeedState{}
			m.userState[email] = state
		}

		if now.Before(state.limitedUntil) {
			continue
		}

		for _, rule := range m.rules {
			thresholdBytes := int64(rule.ThresholdMbps * 1000000 / 8)
			exceeds := speed > thresholdBytes

			switch rule.Type {
			case "sustained":
				m.evalSustained(email, state, rule, exceeds, now)
			case "burst":
				m.evalBurst(email, state, rule, exceeds, now, elapsed)
			}

			if now.Before(state.limitedUntil) {
				break
			}
		}
	}

	for email, state := range m.userState {
		if _, active := userDeltas[email]; !active {
			if now.After(state.limitedUntil) {
				delete(m.userState, email)
			}
		}
	}
}

func (m *SpeedMonitor) evalSustained(email string, state *userSpeedState, rule AutoSpeedLimitRule, exceeds bool, now time.Time) {
	if !exceeds {
		state.sustainedStart = time.Time{}
		return
	}
	if state.sustainedStart.IsZero() {
		state.sustainedStart = now
		return
	}
	if now.Sub(state.sustainedStart).Seconds() >= float64(rule.SustainedSeconds) {
		m.applyLimit(email, state, rule, now)
		state.sustainedStart = time.Time{}
	}
}

func (m *SpeedMonitor) evalBurst(email string, state *userSpeedState, rule AutoSpeedLimitRule, exceeds bool, now time.Time, elapsed time.Duration) {
	window := time.Duration(rule.WindowSeconds) * time.Second
	minDuration := time.Duration(rule.SustainedSeconds) * time.Second

	cutoff := now.Add(-window)
	var kept []time.Time
	for _, t := range state.burstEvents {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	state.burstEvents = kept

	if exceeds {
		if state.sustainedStart.IsZero() {
			state.sustainedStart = now
		}
	} else {
		if !state.sustainedStart.IsZero() && now.Sub(state.sustainedStart) >= minDuration {
			state.burstEvents = append(state.burstEvents, now)
		}
		state.sustainedStart = time.Time{}
	}

	if len(state.burstEvents) >= rule.BurstCount {
		m.applyLimit(email, state, rule, now)
		state.burstEvents = nil
		state.sustainedStart = time.Time{}
	}
}

func (m *SpeedMonitor) applyLimit(email string, state *userSpeedState, rule AutoSpeedLimitRule, now time.Time) {
	limitBytes := uint64(rule.LimitMbps * 1000000 / 8)
	duration := time.Duration(rule.LimitDuration) * time.Second
	state.limitedUntil = now.Add(duration)

	l := m.limiter
	l.InboundInfo.Range(func(key, _ interface{}) bool {
		tag := key.(string)
		l.SetUserSpeed(tag, email, limitBytes)
		return true
	})

	log.Printf("[AutoLimit] User %s limited to %.0f Mbps for %ds (rule: %s, threshold: %.0f Mbps)",
		email, rule.LimitMbps, rule.LimitDuration, rule.Type, rule.ThresholdMbps)

	emailCopy := email
	time.AfterFunc(duration, func() {
		l.InboundInfo.Range(func(key, _ interface{}) bool {
			tag := key.(string)
			l.SetUserSpeed(tag, emailCopy, 0)
			return true
		})
		log.Printf("[AutoLimit] User %s speed limit restored", emailCopy)
	})
}
