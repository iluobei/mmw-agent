package discovery

import (
	"os"
	"os/exec"
	"strings"
)

var defaultServiceFiles = []string{
	"/etc/systemd/system/xray.service",
	"/etc/systemd/system/xray@.service",
	"/lib/systemd/system/xray.service",
	"/lib/systemd/system/xray@.service",
	"/usr/lib/systemd/system/xray.service",
	"/usr/lib/systemd/system/xray@.service",
}

// DiscoverFromSystemd parses the xray systemd unit file to extract -config/-confdir.
func DiscoverFromSystemd() XrayPaths {
	content := findServiceFileContent()
	if content == "" {
		return XrayPaths{}
	}
	return parseExecStart(content)
}

func findServiceFileContent() string {
	for _, path := range defaultServiceFiles {
		data, err := os.ReadFile(path)
		if err == nil {
			return string(data)
		}
	}

	// fallback: ask systemd for the unit file path
	out, err := exec.Command("systemctl", "show", "xray", "-p", "FragmentPath", "--value").Output()
	if err != nil {
		return ""
	}
	path := strings.TrimSpace(string(out))
	if path == "" {
		return ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(data)
}

func parseExecStart(unitContent string) XrayPaths {
	var execStart string
	for _, line := range strings.Split(unitContent, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "ExecStart=") {
			execStart = strings.TrimPrefix(line, "ExecStart=")
		}
	}
	if execStart == "" {
		return XrayPaths{}
	}

	// strip leading "-" (ignore-exit-code prefix)
	execStart = strings.TrimLeft(execStart, "-")

	args := strings.Fields(execStart)
	if len(args) < 2 {
		return XrayPaths{}
	}

	// skip argv[0]
	return ParseXrayArgs(args[1:])
}
