package xrayctl

import (
	"fmt"
	"log"
	"os/exec"
	"syscall"
	"time"

	"mmw-agent/internal/discovery"
)

// RestartXray restarts xray using the specified method.
// Supported methods: "auto", "systemctl", "signal", "custom".
func RestartXray(method, customCmd string) error {
	if method == "" || method == "auto" {
		method = detectMethod()
	}

	log.Printf("[XrayCtl] Restarting xray via %s", method)

	switch method {
	case "systemctl":
		return runCmd("systemctl", "restart", "xray")
	case "signal":
		return restartViaSignal()
	case "custom":
		if customCmd == "" {
			return fmt.Errorf("restart_command is empty")
		}
		return runCmd("bash", "-c", customCmd)
	default:
		return runCmd("systemctl", "restart", "xray")
	}
}

func detectMethod() string {
	// check if systemctl manages xray
	if err := exec.Command("systemctl", "is-enabled", "xray").Run(); err == nil {
		return "systemctl"
	}
	if len(discovery.FindXrayPIDs()) > 0 {
		return "signal"
	}
	return "systemctl"
}

func restartViaSignal() error {
	pids := discovery.FindXrayPIDs()
	if len(pids) == 0 {
		return fmt.Errorf("no running xray process found")
	}

	// save cmdline before killing
	paths := discovery.ParseXrayCmdline(pids[0])
	xrayBin := findXrayBinary(pids[0])

	// send SIGTERM and wait
	for _, pid := range pids {
		_ = syscall.Kill(pid, syscall.SIGTERM)
	}
	time.Sleep(500 * time.Millisecond)

	// relaunch with original args
	if xrayBin == "" {
		return fmt.Errorf("cannot determine xray binary path")
	}

	args := []string{"run"}
	if paths.ConfigPath != "" {
		args = append(args, "-config", paths.ConfigPath)
	}
	if paths.ConfDir != "" {
		args = append(args, "-confdir", paths.ConfDir)
	}

	cmd := exec.Command(xrayBin, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("relaunch xray: %w", err)
	}

	// detach — don't wait for the process
	go func() { _ = cmd.Wait() }()
	return nil
}

func findXrayBinary(pid int) string {
	link := fmt.Sprintf("/proc/%d/exe", pid)
	target, err := exec.Command("readlink", "-f", link).Output()
	if err != nil {
		return ""
	}
	result := string(target)
	if len(result) > 0 && result[len(result)-1] == '\n' {
		result = result[:len(result)-1]
	}
	return result
}

func runCmd(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s %v: %s (%w)", name, args, string(out), err)
	}
	return nil
}
