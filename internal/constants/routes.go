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
	PathChildXrayTestCfg  = "/api/child/xray/test-config"
	PathChildNginxInstall = "/api/child/nginx/install"
	PathChildNginxRemove  = "/api/child/nginx/remove"
	PathChildNginxConfig  = "/api/child/nginx/config"
	PathChildNginxCfgFile = "/api/child/nginx/config-files"
	PathChildSystemInfo   = "/api/child/system/info"
	// PathChildSystemNICs 列出本机启用中的网卡地址,供主控在配置 xray 出站 sendThrough
	// (多 IP 机器指定从哪个源 IP 出站)时给用户选。旧版 agent 无此路由,主控据此降级为手填。
	PathChildSystemNICs = "/api/child/system/nics"
	// PathChildLogs 让主控拉取本机日志:service=agent 读 agent 自身的日志文件(lumberjack),
	// service=xray/nginx 走 journalctl。旧版 agent 无此路由,主控据此降级提示"版本过低"。
	PathChildLogs = "/api/child/logs"
	PathChildInbounds     = "/api/child/inbounds"
	PathChildOutbounds    = "/api/child/outbounds"
	PathChildRouting      = "/api/child/routing"
	PathChildScan         = "/api/child/scan"
	PathChildCertDeploy   = "/api/child/cert/deploy"
	PathChildNginxSetup   = "/api/child/nginx/setup-ssl"
	// PathChildNginxServersList 列出 setup-ssl 实际写入的 servers/ 目录里所有 *.conf,
	// 用于前端在 wss 提交前检测目标域名是否已被(reality / 旧 wss)占用。
	PathChildNginxServersList = "/api/child/nginx/servers-list"
	PathChildDomainProbe       = "/api/child/domains/latency"
	PathChildNginxClearStream  = "/api/child/nginx/clear-stream-port"
	PathChildValidateSite      = "/api/child/validate-site"
	PathChildLimiter           = "/api/child/limiter"
	PathChildSwitchXrayMode    = "/api/child/agent/switch-xray-mode"
	PathChildSwitchListenPort  = "/api/child/agent/switch-listen-port"
	PathChildUpdateMasterURL   = "/api/child/agent/update-master-url"
	PathChildTakeoverXray      = "/api/child/external-xray/takeover"
	// PathChildBatchApply 一次性提交多个 inbound add-client + routing rule add_user 改动,
	// 在 inboundsMu 锁内单次读 config + 单次写盘 + per-inbound runtime apply 完成。
	// 用于套餐绑用户场景:同台 server 上多个 routed 节点的所有改动合并成 1 次 round-trip。
	PathChildBatchApply        = "/api/child/batch-apply"

	// WARP 出站管理 — 每个 agent 各自注册 Cloudflare WARP 账号,本机 xray 双 outbound(warp-v4/warp-v6)
	PathChildWarpInstall = "/api/child/warp/install"
	PathChildWarpStatus  = "/api/child/warp/status"
	PathChildWarpLicense = "/api/child/warp/license"
	PathChildWarpRemove  = "/api/child/warp/remove"
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
