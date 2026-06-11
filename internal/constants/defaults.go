package constants

import "time"

const (
	AgentUserAgent = "miaomiaowux/0.1"
)

const (
	HeaderAuthorization = "Authorization"
	HeaderContentType   = "Content-Type"
	HeaderMMRemoteToken = "MM-Remote-Token"
	HeaderUserAgent     = "User-Agent"
	ContentTypeJSON     = "application/json"
	BearerPrefix        = "Bearer "
)

const (
	ConnectionModeAuto = "auto"
	ConnectionModePull = "pull"
)

const (
	DefaultListenPort = "23889"
)

const (
	DefaultTrafficReportInterval = 5 * time.Second
	DefaultSpeedReportInterval   = 3 * time.Second
	DefaultHTTPClientTimeout     = 10 * time.Second
	DefaultReadTimeout           = 30 * time.Second
	DefaultShutdownTimeout       = 10 * time.Second
	DefaultRPCShortTimeout       = 5 * time.Second
)

const (
	DefaultMetricsHost   = "127.0.0.1"
	DefaultMetricsPort   = 38889
	DefaultMetricsListen = "127.0.0.1:38889"
	LocalhostIP          = "127.0.0.1"
)

const (
	WebSocketMaxConsecutiveFailures = 5
	WebSocketMaxAuthFailures        = 10
	AuthFailureSleepBackoff         = 30 * time.Minute
	PullModeTrafficReportThreshold  = 30 * time.Second
)

const (
	ReconnectBaseBackoff        = 5 * time.Second
	ReconnectMaxBackoff         = 5 * time.Minute
	AuthFailureBackoffStep      = 30 * time.Second
	AuthFailureMaxBackoff       = 10 * time.Minute
	AutoModePullFallbackBackoff = 30 * time.Second
	WebSocketHandshakeTimeout   = 10 * time.Second
	WebSocketReadDeadline       = 10 * time.Second
	WebSocketHeartbeatInterval  = 30 * time.Second
	WebSocketIdleDeadline       = 5 * time.Minute
	// 单次 WriteMessage 上限。没有这个会卡到 TCP retransmission(~15min),
	// 直接拖死 runMessageLoop → Stop() wg.Wait 永久阻塞 → systemctl restart 假死。
	WebSocketWriteDeadline = 10 * time.Second
	// Stop() 等 goroutine 退出的兜底超时,超过就放弃 + log,避免 systemd 等到 TimeoutStopSec。
	AgentStopTimeout = 5 * time.Second
)

const (
	DomainProbeDefaultTimeoutMS = 2000
	DomainProbeMinTimeoutMS     = 200
	DomainProbeMaxTimeoutMS     = 10000
	DomainProbeMaxCount         = 200
	DomainProbeConcurrency      = 16
)

var (
	NginxPrimaryPrefixDir = "/usr/local/nginx"

	DefaultXrayConfigPaths = []string{
		"/usr/local/etc/xray/config.json",
		"/etc/xray/config.json",
		"/opt/xray/config.json",
	}
	XrayConfigDirPaths = []string{
		"/usr/local/etc/xray",
		"/etc/xray",
		"/opt/xray",
	}
	DefaultNginxConfigPaths = []string{
		"/etc/nginx/nginx.conf",
		"/usr/local/nginx/conf/nginx.conf",
	}
	NginxConfigDirPaths = []string{
		"/etc/nginx",
		"/etc/nginx/sites-available",
		"/etc/nginx/sites-enabled",
		"/etc/nginx/conf.d",
		"/usr/local/nginx/conf",
	}
	NginxSSLServerDirPaths = []string{
		"/usr/local/nginx/servers",
		"/usr/local/nginx/conf/servers",
		"/etc/nginx/servers",
		"/etc/nginx/conf.d",
	}
	XrayBinarySearchPaths = []string{
		"/usr/local/bin/xray",
		"/usr/bin/xray",
		"/opt/xray/xray",
	}
	NginxBinarySearchPaths = []string{
		"/usr/local/nginx/sbin/nginx",
		"nginx",
	}
)
