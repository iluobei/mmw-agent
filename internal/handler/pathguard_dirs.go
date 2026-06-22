package handler

import (
	"path/filepath"

	"mmw-agent/internal/constants"
)

// nginxWriteDirs 汇总 nginx 配置的合法写入根目录。主控正常下发的 nginx 配置只会落在这些目录;
// 任何越界路径(如 /etc/cron.d)都会被 setNginxConfig / saveNginxConfigFile 拒绝。
func nginxWriteDirs() []string {
	dirs := append([]string{}, constants.NginxConfigDirPaths...)
	dirs = append(dirs, constants.NginxSSLServerDirPaths...)
	dirs = append(dirs, constants.NginxPrimaryPrefixDir)
	for _, p := range constants.DefaultNginxConfigPaths {
		dirs = append(dirs, filepath.Dir(p))
	}
	return dirs
}

// xrayWriteDirs 汇总 xray 配置的合法写入根目录。
func xrayWriteDirs() []string {
	dirs := append([]string{}, constants.XrayConfigDirPaths...)
	for _, p := range constants.DefaultXrayConfigPaths {
		dirs = append(dirs, filepath.Dir(p))
	}
	return dirs
}
