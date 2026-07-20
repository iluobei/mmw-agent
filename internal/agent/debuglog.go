package agent

import (
	"log"
	"sync/atomic"
)

// debugLogEnabled 由主控通过 config_update("agent_log_enabled") 下发控制,默认关闭。
// 用来 gate agent 侧流量上报等高频日志,避免刷屏。与主控 internal/agentlog 语义一致。
var debugLogEnabled atomic.Bool

func setDebugLogEnabled(v bool) { debugLogEnabled.Store(v) }

// debugLogf 仅在 debug 开关开启时打印(高频日志专用)。
func debugLogf(format string, args ...any) {
	if debugLogEnabled.Load() {
		log.Printf(format, args...)
	}
}
