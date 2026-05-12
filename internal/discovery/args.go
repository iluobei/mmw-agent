package discovery

import "strings"

// XrayPaths holds -config and -confdir values parsed from xray command-line arguments.
type XrayPaths struct {
	ConfigPath string
	ConfDir    string
}

// ParseXrayArgs extracts -config/-c and -confdir/-d values from a slice of
// command-line arguments (e.g. split from /proc/<pid>/cmdline or ExecStart).
func ParseXrayArgs(args []string) XrayPaths {
	var p XrayPaths
	for i := 0; i < len(args); i++ {
		arg := args[i]

		// handle -flag=value form
		if k, v, ok := strings.Cut(arg, "="); ok {
			switch k {
			case "-config", "-c", "--config":
				p.ConfigPath = v
			case "-confdir", "-d", "--confdir":
				p.ConfDir = v
			}
			continue
		}

		// handle -flag value form (next arg is the value)
		switch arg {
		case "-config", "-c", "--config":
			if i+1 < len(args) {
				i++
				p.ConfigPath = args[i]
			}
		case "-confdir", "-d", "--confdir":
			if i+1 < len(args) {
				i++
				p.ConfDir = args[i]
			}
		}
	}
	return p
}
