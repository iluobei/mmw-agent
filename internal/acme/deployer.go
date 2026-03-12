package acme

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// DeployCertFiles writes cert and key PEM to the specified paths.
func DeployCertFiles(certPEM, keyPEM, certPath, keyPath string) error {
	if certPath == "" || keyPath == "" {
		return fmt.Errorf("deploy paths are required")
	}

	if err := os.MkdirAll(filepath.Dir(certPath), 0755); err != nil {
		return fmt.Errorf("create cert dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(keyPath), 0755); err != nil {
		return fmt.Errorf("create key dir: %w", err)
	}

	if err := os.WriteFile(certPath, []byte(certPEM), 0644); err != nil {
		return fmt.Errorf("write cert: %w", err)
	}
	if err := os.WriteFile(keyPath, []byte(keyPEM), 0600); err != nil {
		return fmt.Errorf("write key: %w", err)
	}
	return nil
}

// ReloadNginx sends a reload signal to nginx.
func ReloadNginx() error {
	cmd := exec.Command("nginx", "-s", "reload")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("nginx reload: %s: %w", string(output), err)
	}
	return nil
}

// RestartXray restarts the xray service via systemctl.
func RestartXray() error {
	cmd := exec.Command("systemctl", "restart", "xray")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("xray restart: %s: %w", string(output), err)
	}
	return nil
}

// Deploy writes cert files and optionally reloads services.
func Deploy(certPEM, keyPEM, certPath, keyPath, reloadTarget string) error {
	if err := DeployCertFiles(certPEM, keyPEM, certPath, keyPath); err != nil {
		return err
	}

	switch reloadTarget {
	case "nginx":
		return ReloadNginx()
	case "xray":
		return RestartXray()
	case "both":
		if err := ReloadNginx(); err != nil {
			return err
		}
		return RestartXray()
	}
	return nil
}
