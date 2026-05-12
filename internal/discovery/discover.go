package discovery

import (
	"os"

	"mmw-agent/internal/constants"
)

// Discover finds xray config paths using a 3-tier approach:
//  1. Running process /proc/<pid>/cmdline
//  2. Systemd unit file ExecStart
//  3. Static default paths
func Discover() XrayPaths {
	// Tier 1: running process
	if p := DiscoverFromProcess(); p.ConfigPath != "" || p.ConfDir != "" {
		return p
	}

	// Tier 2: systemd unit file
	if p := DiscoverFromSystemd(); p.ConfigPath != "" || p.ConfDir != "" {
		return p
	}

	// Tier 3: static paths
	return discoverFromStaticPaths()
}

func discoverFromStaticPaths() XrayPaths {
	for _, path := range constants.DefaultXrayConfigPaths {
		if _, err := os.Stat(path); err == nil {
			return XrayPaths{ConfigPath: path}
		}
	}
	return XrayPaths{}
}
