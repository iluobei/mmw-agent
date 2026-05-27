package main

import (
	"encoding/json"
	"testing"
)

func TestMergeXrayConfig_PreservesUserInboundsAndOutbounds(t *testing.T) {
	existing := `{
		"log": {"loglevel": "warning"},
		"inbounds": [
			{"tag": "api", "port": 46736, "protocol": "dokodemo-door"},
			{"tag": "vless-443", "port": 443, "protocol": "vless"}
		],
		"outbounds": [
			{"tag": "direct", "protocol": "freedom"},
			{"tag": "block", "protocol": "blackhole"},
			{"tag": "routed:p1:hk", "protocol": "vless"}
		],
		"routing": {
			"domainStrategy": "IPIfNonMatch",
			"rules": [
				{"type":"field","inboundTag":["api"],"outboundTag":"api"},
				{"type":"field","marktag":"ban_bt","protocol":["bittorrent"],"outboundTag":"block"},
				{"type":"field","user":["alice"],"outboundTag":"routed:p1:hk"}
			]
		}
	}`
	template := `{
		"log": {"loglevel": "error"},
		"inbounds": [
			{"tag": "tunnel-in", "port": 443, "protocol": "tunnel"},
			{"tag": "api", "port": 46736, "protocol": "dokodemo-door"}
		],
		"outbounds": [
			{"tag": "direct", "protocol": "freedom"},
			{"tag": "block", "protocol": "blackhole"},
			{"tag": "nginx", "protocol": "freedom"}
		],
		"routing": {
			"domainStrategy": "IPIfNonMatch",
			"rules": [
				{"inboundTag":["tunnel-in"],"outboundTag":"direct"},
				{"type":"field","inboundTag":["api"],"outboundTag":"api"},
				{"type":"field","marktag":"ban_bt","protocol":["bittorrent"],"outboundTag":"block"}
			]
		}
	}`

	merged, err := mergeXrayConfig([]byte(existing), []byte(template))
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(merged, &m); err != nil {
		t.Fatalf("unmarshal merged: %v", err)
	}

	// 1. inbounds 应同时包含 template 的 tunnel-in/api 和 existing 独有的 vless-443
	wantInboundTags := map[string]bool{"tunnel-in": false, "api": false, "vless-443": false}
	for _, ib := range m["inbounds"].([]any) {
		tag, _ := ib.(map[string]any)["tag"].(string)
		if _, ok := wantInboundTags[tag]; ok {
			wantInboundTags[tag] = true
		}
	}
	for tag, found := range wantInboundTags {
		if !found {
			t.Errorf("inbound tag %q missing after merge", tag)
		}
	}

	// 2. outbounds 应同时包含 template 的 direct/block/nginx 和 existing 独有的 routed:p1:hk
	wantOutboundTags := map[string]bool{"direct": false, "block": false, "nginx": false, "routed:p1:hk": false}
	for _, ob := range m["outbounds"].([]any) {
		tag, _ := ob.(map[string]any)["tag"].(string)
		if _, ok := wantOutboundTags[tag]; ok {
			wantOutboundTags[tag] = true
		}
	}
	for tag, found := range wantOutboundTags {
		if !found {
			t.Errorf("outbound tag %q missing after merge", tag)
		}
	}

	// 3. routing.rules 应保留 existing 独有的 user=alice 路由(无 marktag),并且 ban_bt 只出现一次
	routing := m["routing"].(map[string]any)
	rules := routing["rules"].([]any)
	banBtCount := 0
	foundUserAlice := false
	for _, r := range rules {
		rm, ok := r.(map[string]any)
		if !ok {
			continue
		}
		if mt, _ := rm["marktag"].(string); mt == "ban_bt" {
			banBtCount++
		}
		if users, _ := rm["user"].([]any); len(users) > 0 {
			if u, _ := users[0].(string); u == "alice" {
				foundUserAlice = true
			}
		}
	}
	if banBtCount != 1 {
		t.Errorf("ban_bt rule should appear exactly once after merge, got %d", banBtCount)
	}
	if !foundUserAlice {
		t.Errorf("user=alice routing rule lost after merge")
	}

	// 4. domainStrategy 用 template 的
	if got, _ := routing["domainStrategy"].(string); got != "IPIfNonMatch" {
		t.Errorf("domainStrategy = %q, want IPIfNonMatch", got)
	}

	// 5. log.loglevel 用 template 的 (顶层非 inbounds/outbounds/routing 字段)
	logBlock := m["log"].(map[string]any)
	if got, _ := logBlock["loglevel"].(string); got != "error" {
		t.Errorf("log.loglevel = %q, want error (template override)", got)
	}
}

func TestMergeXrayConfig_TemplateInboundReplacesSameTag(t *testing.T) {
	existing := `{"inbounds":[{"tag":"api","port":1234}],"outbounds":[]}`
	template := `{"inbounds":[{"tag":"api","port":46736}],"outbounds":[]}`
	merged, err := mergeXrayConfig([]byte(existing), []byte(template))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	json.Unmarshal(merged, &m)
	ibs := m["inbounds"].([]any)
	if len(ibs) != 1 {
		t.Fatalf("expect 1 inbound, got %d", len(ibs))
	}
	port, _ := ibs[0].(map[string]any)["port"].(float64)
	if port != 46736 {
		t.Errorf("template port should win, got %v", port)
	}
}
