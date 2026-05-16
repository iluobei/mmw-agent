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

func buildCoreConfig(configPath string) (*core.Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	pbConfig, err := confserial.LoadJSONConfig(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	// Prepend our custom dispatcher config BEFORE the official one.
	// This ensures RequireFeatures resolves our custom dispatcher first.
	customApps := []*serial.TypedMessage{
		serial.ToTypedMessage(&mydispatcher.Config{}),
		serial.ToTypedMessage(&officialdispatcher.Config{
			Settings: &officialdispatcher.SessionConfig{},
		}),
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
