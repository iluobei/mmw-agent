package handler

import (
	"os/exec"
	"strings"

	"mmw-agent/internal/constants"
	"mmw-agent/internal/util"
)

// Docker 镜像里没有 systemd,所有 systemctl 控制 nginx/xray 的逻辑在 docker 模式下走 binary 直接命令。
// 裸机部署不变(走原 systemctl 路径)。设计跟主控 miaomiaowuX ensureNginxRunning 完全对称。

// findNginxBinary 找 nginx 可执行文件路径,跟 reloadNginx 复用同一搜索列表
// (constants.NginxBinarySearchPaths 含 /usr/local/nginx/sbin/nginx, /usr/sbin/nginx 等)。
func findNginxBinary() string {
	for _, bin := range constants.NginxBinarySearchPaths {
		if p, err := exec.LookPath(bin); err == nil {
			return p
		}
	}
	return ""
}

// nginxIsActive 判断 nginx 是否在跑:
//   - docker:走 pgrep(容器内 systemctl 不可用,且 pgrep 比 systemctl 更直观)
//   - 裸机:systemctl is-active
func nginxIsActive() bool {
	if util.IsDocker() {
		return exec.Command("pgrep", "-x", "nginx").Run() == nil
	}
	out, err := exec.Command("systemctl", "is-active", "nginx").Output()
	return err == nil && strings.TrimSpace(string(out)) == "active"
}

// nginxStart 启动 nginx:
//   - docker:直接 `nginx`(默认 daemon 模式 fork 自后台);失败则 fallback 找 binary 跑
//   - 裸机:systemctl start nginx
func nginxStart() error {
	if util.IsDocker() {
		if bin := findNginxBinary(); bin != "" {
			return exec.Command(bin).Run()
		}
		return exec.Command("nginx").Run()
	}
	return exec.Command("systemctl", "start", "nginx").Run()
}

// nginxStop 停止 nginx:
//   - docker:`nginx -s stop`(优雅退出);找不到 binary 走 pkill 兜底
//   - 裸机:systemctl stop nginx
func nginxStop() error {
	if util.IsDocker() {
		if bin := findNginxBinary(); bin != "" {
			if err := exec.Command(bin, "-s", "stop").Run(); err == nil {
				return nil
			}
		}
		return exec.Command("pkill", "-x", "nginx").Run()
	}
	return exec.Command("systemctl", "stop", "nginx").Run()
}
