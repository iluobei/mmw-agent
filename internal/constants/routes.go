package constants

const (
	PathHealth = "/health"
)

const (
	PathChildTraffic      = "/api/child/traffic"
	PathChildSpeed        = "/api/child/speed"
	PathChildServiceStats = "/api/child/services/status"
	PathChildServiceCtl   = "/api/child/services/control"
	PathChildXrayInstall  = "/api/child/xray/install"
	PathChildXrayRemove   = "/api/child/xray/remove"
	PathChildXrayConfig   = "/api/child/xray/config"
	PathChildXraySysCfg   = "/api/child/xray/system-config"
	PathChildXrayCfgFiles = "/api/child/xray/config-files"
	PathChildNginxInstall = "/api/child/nginx/install"
	PathChildNginxRemove  = "/api/child/nginx/remove"
	PathChildNginxConfig  = "/api/child/nginx/config"
	PathChildNginxCfgFile = "/api/child/nginx/config-files"
	PathChildSystemInfo   = "/api/child/system/info"
	PathChildInbounds     = "/api/child/inbounds"
	PathChildOutbounds    = "/api/child/outbounds"
	PathChildRouting      = "/api/child/routing"
	PathChildScan         = "/api/child/scan"
	PathChildCertDeploy   = "/api/child/cert/deploy"
	PathChildNginxSetup   = "/api/child/nginx/setup-ssl"
	PathChildDomainProbe       = "/api/child/domains/latency"
	PathChildNginxClearStream  = "/api/child/nginx/clear-stream-port"
	PathChildValidateSite      = "/api/child/validate-site"
	PathChildLimiter           = "/api/child/limiter"
	PathChildSwitchXrayMode    = "/api/child/agent/switch-xray-mode"
	PathChildSwitchListenPort  = "/api/child/agent/switch-listen-port"
	PathChildUpdateMasterURL   = "/api/child/agent/update-master-url"
	PathChildTakeoverXray      = "/api/child/external-xray/takeover"
)

const (
	PathChildXrayInstallStream    = "/api/child/xray/install-stream"
	PathChildXrayRemoveStream     = "/api/child/xray/remove-stream"
	PathChildNginxInstallSSE      = "/api/child/nginx/install-stream"
	PathChildNginxRemoveSSE       = "/api/child/nginx/remove-stream"
	PathChildAgentUpgradeStream   = "/api/child/agent/upgrade-stream"
	PathChildAgentUninstallStream = "/api/child/agent/uninstall-stream"
)

const (
	PathRemoteWebSocket = "/api/remote/ws"
	PathRemoteHeartbeat = "/api/remote/heartbeat"
	PathRemoteTraffic   = "/api/remote/traffic"
	PathRemoteSpeed     = "/api/remote/speed"
)
