package embedded

import (
	"bytes"
	"encoding/json"
	"os"

	officialstats "github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	confserial "github.com/xtls/xray-core/infra/conf/serial"

	officialdispatcher "github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/metrics"
	"github.com/xtls/xray-core/app/policy"

	mydispatcher "mmw-agent/internal/dispatcher"
)

// TestConfigJSON 用 xray-core 库语义验证一份 JSON 配置:
//   - 解析失败 → 返回 error,内容包含 xray-core 抛出的字段路径/类型错误
//   - 解析成功 → 返回 nil
//
// 不会绑定端口、不会和正在运行的内嵌 xray instance 冲突 — 只走 conf parsing。
// 实现等价 xray 二进制的 -test flag 内部所做的事(LoadConfig,不 Build instance)。
func TestConfigJSON(jsonData []byte) error {
	_, err := confserial.LoadJSONConfig(bytes.NewReader(jsonData))
	return err
}

// AccessLogPath 非空时,buildCoreConfig 会把内嵌 xray 的 access log 强制落到这个文件
// (覆盖下发配置里的 log.access)。由 main 按 agent 日志目录设置。
//
// 目的:内嵌模式下 access log 默认直写 stdout,面板「查看 xray 日志」查 journalctl -u xray
// (不存在的 unit)看不到。落文件后面板直接读它,不依赖 systemd。
var AccessLogPath string

// injectAccessLog 把 log.access 覆盖为 AccessLogPath。只动 access,不碰 loglevel ——
// access log(accepted/rejected)在 xray 里独立于 loglevel,用户设 error 也照打。
// 解析失败一律返回原始 data,绝不因为这个可选特性阻断 xray 启动。
func injectAccessLog(data []byte) []byte {
	if AccessLogPath == "" {
		return data
	}
	var m map[string]any
	if json.Unmarshal(data, &m) != nil {
		return data
	}
	logCfg, _ := m["log"].(map[string]any)
	if logCfg == nil {
		logCfg = map[string]any{}
		m["log"] = logCfg
	}
	logCfg["access"] = AccessLogPath
	out, err := json.Marshal(m)
	if err != nil {
		return data
	}
	return out
}

func buildCoreConfig(configPath string) (*core.Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	data = injectAccessLog(data)

	pbConfig, err := confserial.LoadJSONConfig(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	// 只注册自定义 dispatcher,不再注册 officialdispatcher。
	// 自定义 dispatcher.Type() 返回 routing.DispatcherType(),会被作为标准 routing.Dispatcher feature 解析,
	// 所有走 routing.Dispatcher 的流量进入自定义实现 → limiter / per-user RateWriter / user-traffic stats 才能挂得上。
	// 若同时注册官方 dispatcher,xray-core 内部会以官方实现为准,limiter 钩子完全无效。
	customApps := []*serial.TypedMessage{
		serial.ToTypedMessage(&mydispatcher.Config{}),
		serial.ToTypedMessage(&officialstats.Config{}),
		serial.ToTypedMessage(&policy.Config{
			Level: map[uint32]*policy.Policy{
				0: {
					Stats: &policy.Policy_Stats{
						UserUplink:   true,
						UserDownlink: true,
						UserOnline:   true,
					},
				},
			},
			System: &policy.SystemPolicy{
				Stats: &policy.SystemPolicy_Stats{
					InboundUplink:    true,
					InboundDownlink:  true,
					OutboundUplink:   true,
					OutboundDownlink: true,
				},
			},
		}),
	}

	// Remove existing dispatcher/stats/policy configs from parsed config
	// to avoid duplicates, then prepend ours.
	var filtered []*serial.TypedMessage
	skipTypes := map[string]bool{
		serial.GetMessageType(&officialdispatcher.Config{}): true,
		serial.GetMessageType(&officialstats.Config{}):      true,
		serial.GetMessageType(&policy.Config{}):              true,
		serial.GetMessageType(&mydispatcher.Config{}):        true,
		serial.GetMessageType(&metrics.Config{}):             true,
	}
	for _, app := range pbConfig.App {
		if !skipTypes[app.Type] {
			filtered = append(filtered, app)
		}
	}

	pbConfig.App = append(customApps, filtered...)

	return pbConfig, nil
}
