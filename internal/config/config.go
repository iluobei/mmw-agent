package config

import (
	"log"
	"os"
	"time"

	"mmw-agent/internal/constants"
	"mmw-agent/internal/discovery"

	"gopkg.in/yaml.v3"
)

const AgentUserAgent = constants.AgentUserAgent

// Config 保存 agent 的运行配置。
type Config struct {
	MasterURL             string        `yaml:"master_url"`
	Token                 string        `yaml:"token"`
	ConnectionMode        string        `yaml:"connection_mode"`
	ListenPort            string        `yaml:"listen_port"`
	XrayMode              string        `yaml:"xray_mode"` // "external" (default) or "embedded"
	StealMode             string        `yaml:"steal_mode"` // "", "tunnel", "fallback"
	XrayServers           []XrayServer  `yaml:"xray_servers"`
	TrafficReportInterval time.Duration `yaml:"traffic_report_interval"`
	SpeedReportInterval   time.Duration `yaml:"speed_report_interval"`
	RestartMethod         string        `yaml:"restart_method"`
	RestartCommand        string        `yaml:"restart_command"`
	MasterPublicKey       string        `yaml:"master_public_key"`
	LogPath               string        `yaml:"log_path"` // 日志文件路径，默认 /var/log/mmw-agent/mmw-agent.log
	// HidePortOnWS 开启时:WS 连接可用期间关闭入站监听端口(隐藏 agent),WS 断开时立即重开以保证
	// 主控 HTTP/pull 回退可达。*bool 区分"未配置"(默认 true)与显式 false。
	HidePortOnWS *bool `yaml:"hide_port_on_ws"`
	// XrayAuthorized 是主控按许可证「服务器配额」下发的运行授权:超出配额的服务器为 false → agent 停 xray。
	// 主控通过 config_update 下发并由 agent 落盘,重启时据此决定是否启动 xray(实现「重启立即检查」)。
	// *bool 区分"未配置/首次启动"(nil = 默认授权,先跑)与主控显式判定的 true/false。
	XrayAuthorized *bool `yaml:"xray_authorized"`
}

// DefaultLogPath 是 agent 日志文件默认路径（lumberjack 在同目录轮转生成备份）。
const DefaultLogPath = "/var/log/mmw-agent/mmw-agent.log"

// XrayServer 表示本机 Xray 节点配置。
type XrayServer struct {
	Name       string `yaml:"name"`
	ConfigPath string `yaml:"config_path"`
	ConfDir    string `yaml:"conf_dir"`
}

// DefaultXrayConfigPaths 是默认的 Xray 配置搜索路径。
var DefaultXrayConfigPaths = append([]string(nil), constants.DefaultXrayConfigPaths...)

// 从 YAML 文件加载配置。
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	config.applyDefaults()
	return &config, nil
}

// 从环境变量构造配置（不含默认值，用于 Merge）。
func fromEnvRaw() *Config {
	config := &Config{
		MasterURL:      os.Getenv("MMWX_MASTER_URL"),
		Token:          os.Getenv("MMWX_TOKEN"),
		ConnectionMode: os.Getenv("MMWX_CONNECTION_MODE"),
		ListenPort:     os.Getenv("MMWX_LISTEN_PORT"),
		XrayMode:       os.Getenv("MMWX_XRAY_MODE"),
		RestartMethod:   os.Getenv("MMWX_RESTART_METHOD"),
		RestartCommand:  os.Getenv("MMWX_RESTART_COMMAND"),
		MasterPublicKey: os.Getenv("MMWX_MASTER_PUBLIC_KEY"),
		LogPath:         os.Getenv("MMWX_LOG_PATH"),
	}

	if xrayConfig := os.Getenv("MMWX_XRAY_CONFIG"); xrayConfig != "" {
		server := XrayServer{Name: "primary", ConfigPath: xrayConfig}
		if confDir := os.Getenv("MMWX_XRAY_CONFDIR"); confDir != "" {
			server.ConfDir = confDir
		}
		config.XrayServers = []XrayServer{server}
	}

	if interval := os.Getenv("MMWX_TRAFFIC_INTERVAL"); interval != "" {
		if d, err := time.ParseDuration(interval); err == nil {
			config.TrafficReportInterval = d
		}
	}
	if interval := os.Getenv("MMWX_SPEED_INTERVAL"); interval != "" {
		if d, err := time.ParseDuration(interval); err == nil {
			config.SpeedReportInterval = d
		}
	}
	if v := os.Getenv("MMWX_HIDE_PORT_ON_WS"); v != "" {
		b := v == "1" || v == "true"
		config.HidePortOnWS = &b
	}

	return config
}

// 从环境变量构造完整配置（含默认值，独立使用时调用）。
func FromEnv() *Config {
	config := fromEnvRaw()
	config.applyDefaults()
	return config
}

// MergeEnv 将环境变量覆盖到已加载的文件配置上，只覆盖环境变量中实际设置的字段。
func (c *Config) MergeEnv() {
	c.Merge(fromEnvRaw())
}

// 合并环境变量配置到文件配置（环境变量优先）。
func (c *Config) Merge(env *Config) {
	if env.MasterURL != "" {
		c.MasterURL = env.MasterURL
	}
	if env.Token != "" {
		c.Token = env.Token
	}
	if env.ConnectionMode != "" {
		c.ConnectionMode = env.ConnectionMode
	}
	if env.ListenPort != "" {
		c.ListenPort = env.ListenPort
	}
	if env.XrayMode != "" {
		c.XrayMode = env.XrayMode
	}
	if len(env.XrayServers) > 0 {
		c.XrayServers = env.XrayServers
	}
	if env.TrafficReportInterval > 0 {
		c.TrafficReportInterval = env.TrafficReportInterval
	}
	if env.SpeedReportInterval > 0 {
		c.SpeedReportInterval = env.SpeedReportInterval
	}
	if env.RestartMethod != "" {
		c.RestartMethod = env.RestartMethod
	}
	if env.RestartCommand != "" {
		c.RestartCommand = env.RestartCommand
	}
	if env.MasterPublicKey != "" {
		c.MasterPublicKey = env.MasterPublicKey
	}
	if env.LogPath != "" {
		c.LogPath = env.LogPath
	}
	if env.HidePortOnWS != nil {
		c.HidePortOnWS = env.HidePortOnWS
	}
}

// 为空字段填充默认值。
func (c *Config) applyDefaults() {
	if c.ConnectionMode == "" {
		c.ConnectionMode = constants.ConnectionModeAuto
	}
	if c.ListenPort == "" {
		c.ListenPort = constants.DefaultListenPort
	}
	if c.TrafficReportInterval == 0 {
		c.TrafficReportInterval = constants.DefaultTrafficReportInterval
	}
	if c.SpeedReportInterval == 0 {
		c.SpeedReportInterval = constants.DefaultSpeedReportInterval
	}
	if c.RestartMethod == "" {
		c.RestartMethod = "auto"
	}
	if c.XrayMode == "" {
		c.XrayMode = "external"
	}
	if c.LogPath == "" {
		c.LogPath = DefaultLogPath
	}
	if c.HidePortOnWS == nil {
		v := true
		c.HidePortOnWS = &v
	}

	if len(c.XrayServers) == 0 {
		c.XrayServers = c.discoverXrayServers()
	}
}

// 通过 3-tier 方式发现 Xray 配置。
func (c *Config) discoverXrayServers() []XrayServer {
	p := discovery.Discover()
	if p.ConfigPath != "" || p.ConfDir != "" {
		server := XrayServer{
			Name:       "xray-1",
			ConfigPath: p.ConfigPath,
			ConfDir:    p.ConfDir,
		}
		log.Printf("[Config] Auto-discovered xray: config=%s confdir=%s", p.ConfigPath, p.ConfDir)
		return []XrayServer{server}
	}
	return nil
}

// 校验配置是否合法。
func (c *Config) Validate() error {
	if c.ConnectionMode != constants.ConnectionModePull && c.Token == "" {
		// 兼容空 token，实际仅拉取模式可正常工作
	}
	return nil
}
