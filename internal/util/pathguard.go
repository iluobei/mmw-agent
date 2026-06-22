package util

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"path/filepath"
	"strings"
)

// PathWithinDirs 报告 target 是否落在 allowedDirs 任一目录(含其子目录)内。
// 用 filepath.Rel 做目录边界判定,而非 strings.HasPrefix —— 后者会把
// "/etc/nginx-evil/x" 误判为在 "/etc/nginx" 之内。target 必须是绝对路径。
func PathWithinDirs(target string, allowedDirs []string) bool {
	clean := filepath.Clean(target)
	if !filepath.IsAbs(clean) {
		return false
	}
	for _, dir := range allowedDirs {
		rel, err := filepath.Rel(filepath.Clean(dir), clean)
		if err != nil {
			continue
		}
		if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)) {
			return true
		}
	}
	return false
}

// sensitiveCertTargets 列出绝不可作为证书/密钥落地点的目录与文件 —— 写入即可代码执行 / 提权 / 持久化。
var sensitiveCertTargets = []string{
	"/etc/cron.d", "/etc/cron.daily", "/etc/cron.hourly", "/etc/cron.weekly",
	"/etc/cron.monthly", "/etc/crontab", "/var/spool/cron",
	"/etc/systemd", "/lib/systemd", "/usr/lib/systemd", "/etc/init.d",
	"/etc/profile.d", "/etc/sudoers.d", "/etc/sudoers",
	"/etc/ld.so.conf.d", "/etc/ld.so.preload",
	"/root", "/home",
	"/usr/local/bin", "/usr/bin", "/usr/sbin", "/bin", "/sbin",
	"/etc/passwd", "/etc/shadow", "/etc/bash.bashrc",
}

// CertPathSafe 校验证书部署路径不落在敏感目录。证书目标路径管理员可自定义(deploy_cert_path),
// 无法用白名单收死;这里做黑名单兜底,主防御是 ValidateCertKeyPEM(把内容限定为真实证书 / 私钥)。
func CertPathSafe(target string) error {
	clean := filepath.Clean(target)
	if !filepath.IsAbs(clean) {
		return fmt.Errorf("路径必须为绝对路径: %q", target)
	}
	for _, s := range sensitiveCertTargets {
		sc := filepath.Clean(s)
		if clean == sc {
			return fmt.Errorf("拒绝写入敏感位置: %q", clean)
		}
		if rel, err := filepath.Rel(sc, clean); err == nil &&
			rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel) {
			return fmt.Errorf("拒绝写入敏感位置: %q", clean)
		}
	}
	return nil
}

// ValidateCertKeyPEM 确认 certPEM / keyPEM 确为 PEM 编码的证书与私钥。
// 这是 cert 部署的主防御:内容被限定为真实证书 / 私钥后,即便目标路径可控,
// 也无法写入 cron / shell 脚本 / authorized_keys / ELF 等可执行内容。
// 兼容 fullchain(取首个 CERTIFICATE 块)与加密私钥("ENCRYPTED PRIVATE KEY")。
func ValidateCertKeyPEM(certPEM, keyPEM string) error {
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil || block.Type != "CERTIFICATE" {
		return fmt.Errorf("cert_pem 不是合法的 PEM 证书")
	}
	if _, err := x509.ParseCertificate(block.Bytes); err != nil {
		return fmt.Errorf("cert_pem 解析失败: %w", err)
	}
	kb, _ := pem.Decode([]byte(keyPEM))
	if kb == nil || !strings.Contains(kb.Type, "PRIVATE KEY") {
		return fmt.Errorf("key_pem 不是合法的 PEM 私钥")
	}
	return nil
}

// ValidHostname 报告 s 是否为合法主机名(仅字母数字、点、连字符),
// 用于把域名拼进文件路径前的净化,杜绝目录穿越(如 "../../etc/cron.d/x")。
func ValidHostname(s string) bool {
	if s == "" || len(s) > 253 || strings.Contains(s, "..") {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '-') {
			return false
		}
	}
	return true
}
