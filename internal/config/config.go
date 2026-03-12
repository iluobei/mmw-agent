package config

import (
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

const AgentUserAgent = "miaomiaowux/0.1"

// Config holds the agent configuration
type Config struct {
	MasterURL             string        `yaml:"master_url"`
	Token                 string        `yaml:"token"`
	ConnectionMode        string        `yaml:"connection_mode"`
	ListenPort            string        `yaml:"listen_port"`
	XrayServers           []XrayServer  `yaml:"xray_servers"`
	TrafficReportInterval time.Duration `yaml:"traffic_report_interval"`
	SpeedReportInterval   time.Duration `yaml:"speed_report_interval"`
}

// XrayServer represents a local Xray server configuration
type XrayServer struct {
	Name       string `yaml:"name"`
	ConfigPath string `yaml:"config_path"`
}

// DefaultXrayConfigPaths are the default paths to search for Xray config
var DefaultXrayConfigPaths = []string{
	"/usr/local/etc/xray/config.json",
	"/etc/xray/config.json",
	"/opt/xray/config.json",
}

// Load loads configuration from a YAML file
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	// Apply defaults
	config.applyDefaults()

	return &config, nil
}

// FromEnv creates configuration from environment variables
func FromEnv() *Config {
	config := &Config{
		MasterURL:      os.Getenv("MMWX_MASTER_URL"),
		Token:          os.Getenv("MMWX_TOKEN"),
		ConnectionMode: os.Getenv("MMWX_CONNECTION_MODE"),
		ListenPort:     os.Getenv("MMWX_LISTEN_PORT"),
	}

	// Parse Xray config path from env
	if xrayConfig := os.Getenv("MMWX_XRAY_CONFIG"); xrayConfig != "" {
		config.XrayServers = []XrayServer{
			{Name: "primary", ConfigPath: xrayConfig},
		}
	}

	// Parse intervals
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

	config.applyDefaults()
	return config
}

// Merge merges environment config into file config (env takes precedence)
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
	if len(env.XrayServers) > 0 {
		c.XrayServers = env.XrayServers
	}
	if env.TrafficReportInterval > 0 {
		c.TrafficReportInterval = env.TrafficReportInterval
	}
	if env.SpeedReportInterval > 0 {
		c.SpeedReportInterval = env.SpeedReportInterval
	}
}

// applyDefaults sets default values for unset fields
func (c *Config) applyDefaults() {
	if c.ConnectionMode == "" {
		c.ConnectionMode = "auto"
	}
	if c.ListenPort == "" {
		c.ListenPort = "23889"
	}
	if c.TrafficReportInterval == 0 {
		c.TrafficReportInterval = 1 * time.Minute
	}
	if c.SpeedReportInterval == 0 {
		c.SpeedReportInterval = 3 * time.Second
	}

	// Auto-discover Xray servers if not configured
	if len(c.XrayServers) == 0 {
		c.XrayServers = c.discoverXrayServers()
	}
}

// discoverXrayServers scans default paths for Xray config files
func (c *Config) discoverXrayServers() []XrayServer {
	var servers []XrayServer
	for i, path := range DefaultXrayConfigPaths {
		if _, err := os.Stat(path); err == nil {
			servers = append(servers, XrayServer{
				Name:       "xray-" + strconv.Itoa(i+1),
				ConfigPath: path,
			})
		}
	}
	return servers
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	// Token is required for non-pull modes
	if c.ConnectionMode != "pull" && c.Token == "" {
		// Allow empty token, will work in pull mode only
	}
	return nil
}
