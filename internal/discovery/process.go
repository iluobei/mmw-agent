package discovery

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// FindXrayPIDs returns the PIDs of all running xray processes by scanning /proc.
func FindXrayPIDs() []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}

	var pids []int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		if isXrayProcess(pid) {
			pids = append(pids, pid)
		}
	}
	return pids
}

func isXrayProcess(pid int) bool {
	// fast check: /proc/<pid>/comm
	comm, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "comm"))
	if err != nil {
		return false
	}
	if strings.TrimSpace(string(comm)) == "xray" {
		return true
	}

	// fallback: check exe symlink basename
	exe, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(pid), "exe"))
	if err != nil {
		return false
	}
	return filepath.Base(exe) == "xray"
}

// ParseXrayCmdline reads /proc/<pid>/cmdline and extracts -config and -confdir.
func ParseXrayCmdline(pid int) XrayPaths {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
	if err != nil || len(data) == 0 {
		return XrayPaths{}
	}

	// cmdline is NUL-delimited; strip trailing NUL
	data = bytes.TrimRight(data, "\x00")
	args := strings.Split(string(data), "\x00")

	// skip argv[0] (the binary path)
	if len(args) > 1 {
		args = args[1:]
	} else {
		return XrayPaths{}
	}

	return ParseXrayArgs(args)
}

// DiscoverFromProcess finds xray config paths by inspecting running processes.
// Returns paths from the first xray process found.
func DiscoverFromProcess() XrayPaths {
	for _, pid := range FindXrayPIDs() {
		p := ParseXrayCmdline(pid)
		if p.ConfigPath != "" || p.ConfDir != "" {
			return p
		}
	}
	return XrayPaths{}
}
