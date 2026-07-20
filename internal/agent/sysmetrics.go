package agent

import (
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// 伪装探针「真数据」的系统指标采集:CPU 使用率 / 内存 / 硬盘。
// 沿用仓库手撸 /proc 的风格(不引 gopsutil);硬盘用 x/sys/unix.Statfs(已在依赖树)。

// ProbeSysMetrics 是一次采样结果,上报给主控(字段名与主控 remote_ws 解析对齐)。
type ProbeSysMetrics struct {
	CPUPct    float64 `json:"cpu_pct"`
	LoadAvg   string  `json:"loadavg"`
	MemUsed   int64   `json:"mem_used"`
	MemTotal  int64   `json:"mem_total"`
	DiskUsed  int64   `json:"disk_used"`
	DiskTotal int64   `json:"disk_total"`
	// 掩码:agent 只采开启项,主控据此决定填哪些字段(避免未采集的 0 值被当真实数据)。
	HasCPU  bool `json:"has_cpu"`
	HasMem  bool `json:"has_mem"`
	HasDisk bool `json:"has_disk"`
}

// cpuTimes 是 /proc/stat 第一行 cpu 的累计时间片(jiffies)。
type cpuTimes struct {
	idle  uint64 // idle + iowait
	total uint64 // 所有字段之和
}

// sysMetricsCollector 保存上次 CPU 快照用于差分。非并发安全,由单个采集 goroutine 独占。
type sysMetricsCollector struct {
	lastCPU    cpuTimes
	hasLastCPU bool
}

func newSysMetricsCollector() *sysMetricsCollector { return &sysMetricsCollector{} }

// cpuPct 是纯函数:两次 /proc/stat 快照差分算 CPU 使用率百分比。
// busy = Δtotal - Δidle; pct = busy/Δtotal*100。Δtotal<=0(异常/首次)返回 0。
func cpuPct(prev, cur cpuTimes) float64 {
	dTotal := int64(cur.total) - int64(prev.total)
	dIdle := int64(cur.idle) - int64(prev.idle)
	if dTotal <= 0 {
		return 0
	}
	busy := dTotal - dIdle
	if busy < 0 {
		busy = 0
	}
	return float64(busy) / float64(dTotal) * 100
}

// readCPUTimes 解析 /proc/stat 第一行 "cpu  user nice system idle iowait irq softirq steal ..."。
func readCPUTimes() (cpuTimes, bool) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return cpuTimes{}, false
	}
	line := strings.SplitN(string(data), "\n", 2)[0]
	fields := strings.Fields(line)
	if len(fields) < 5 || fields[0] != "cpu" {
		return cpuTimes{}, false
	}
	var t cpuTimes
	for i := 1; i < len(fields); i++ {
		v, err := strconv.ParseUint(fields[i], 10, 64)
		if err != nil {
			continue
		}
		t.total += v
		// 第 4 个字段(idx 4)=idle,第 5 个(idx 5)=iowait
		if i == 4 || i == 5 {
			t.idle += v
		}
	}
	return t, true
}

// readMem 读 /proc/meminfo,返回 (used, total) 字节。used = MemTotal - MemAvailable。
func readMem() (used, total int64, ok bool) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, false
	}
	var memTotal, memAvail int64
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		// 值形如 "16384000 kB"
		valStr := strings.Fields(strings.TrimSpace(parts[1]))
		if len(valStr) == 0 {
			continue
		}
		kb, err := strconv.ParseInt(valStr[0], 10, 64)
		if err != nil {
			continue
		}
		switch key {
		case "MemTotal":
			memTotal = kb * 1024
		case "MemAvailable":
			memAvail = kb * 1024
		}
	}
	if memTotal == 0 {
		return 0, 0, false
	}
	used = memTotal - memAvail
	if used < 0 {
		used = 0
	}
	return used, memTotal, true
}

// readDisk 用 Statfs 读根分区 (used, total) 字节。
func readDisk() (used, total int64, ok bool) {
	var st unix.Statfs_t
	if err := unix.Statfs("/", &st); err != nil {
		return 0, 0, false
	}
	bs := int64(st.Bsize)
	total = int64(st.Blocks) * bs
	avail := int64(st.Bavail) * bs
	used = total - avail
	if total <= 0 {
		return 0, 0, false
	}
	return used, total, true
}

// readLoadAvg 读 /proc/loadavg 原串(1min 5min 15min ...)。
func readLoadAvg() string {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// sample 采集开启项。cpu/mem/disk 分别由开关控制,只采开启的,未开启项 HasX=false。
// CPU% 需两次采样差分:首次调用(无基线)返回 HasCPU=true 但 CPUPct=0 并存基线,下次才有真实值。
func (c *sysMetricsCollector) sample(cpu, mem, disk bool) ProbeSysMetrics {
	var m ProbeSysMetrics
	if cpu {
		if cur, ok := readCPUTimes(); ok {
			if c.hasLastCPU {
				m.CPUPct = cpuPct(c.lastCPU, cur)
			}
			c.lastCPU = cur
			c.hasLastCPU = true
			m.LoadAvg = readLoadAvg()
			m.HasCPU = true
		}
	}
	if mem {
		if used, total, ok := readMem(); ok {
			m.MemUsed, m.MemTotal, m.HasMem = used, total, true
		}
	}
	if disk {
		if used, total, ok := readDisk(); ok {
			m.DiskUsed, m.DiskTotal, m.HasDisk = used, total, true
		}
	}
	return m
}
