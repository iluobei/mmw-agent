package main

import "testing"

// 验证 B3:解析 /proc/meminfo 文本取 MemTotal(kB)→ 字节数。
func TestParseMemTotalBytes(t *testing.T) {
	sample := "MemTotal:        2048000 kB\n" +
		"MemFree:          100000 kB\n" +
		"MemAvailable:    1500000 kB\n"
	got := parseMemTotalBytes(sample)
	want := int64(2048000) * 1024
	if got != want {
		t.Fatalf("MemTotal: got %d, want %d", got, want)
	}
}

func TestParseMemTotalBytesMissing(t *testing.T) {
	if got := parseMemTotalBytes("Foo: 1 kB\nBar: 2 kB\n"); got != 0 {
		t.Fatalf("无 MemTotal 应返回 0, got %d", got)
	}
	if got := parseMemTotalBytes(""); got != 0 {
		t.Fatalf("空输入应返回 0, got %d", got)
	}
}
