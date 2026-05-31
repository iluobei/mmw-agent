package embedded

import (
	"bytes"
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

func buildCoreConfig(configPath string) (*core.Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

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
