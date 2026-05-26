package discovery

import (
	"os"

	"mmw-agent/internal/constants"
)

// Discover finds xray config paths using a 3-tier approach:
//  1. Running process /proc/<pid>/cmdline
//  2. Systemd unit file ExecStart
//  3. Static default paths
//
// 在每一层,如果返回的 ConfigPath 指向一个不存在的文件,直接跳过这一层 — 这避免
// "外置 xray 已被接管到 embedded(原文件已归档),但 systemd unit 没删,Discover 仍指向死路径"
// 的常见场景。
func Discover() XrayPaths {
	// Tier 1: running process
	if p := DiscoverFromProcess(); pathsUsable(p) {
		return p
	}

	// Tier 2: systemd unit file
	if p := DiscoverFromSystemd(); pathsUsable(p) {
		return p
	}

	// Tier 3: static paths
	return discoverFromStaticPaths()
}

// pathsUsable 判断 Discover 返回的路径是否真的指向一个存在的文件。
// ConfigPath 不存在则视为不可用(忽略这一层);ConfDir 不存在不影响(可空)。
func pathsUsable(p XrayPaths) bool {
	if p.ConfigPath == "" && p.ConfDir == "" {
		return false
	}
	if p.ConfigPath != "" {
		if _, err := os.Stat(p.ConfigPath); err != nil {
			return false
		}
	}
	return true
}

func discoverFromStaticPaths() XrayPaths {
	for _, path := range constants.DefaultXrayConfigPaths {
		if _, err := os.Stat(path); err == nil {
			return XrayPaths{ConfigPath: path}
		}
	}
	return XrayPaths{}
}
