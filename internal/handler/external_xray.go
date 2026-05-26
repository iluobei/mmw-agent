package handler

// 处理"接管外置 xray"的运行时迁移:
//   - 探测正在跑的 xray 进程 / systemd unit,拿到 -config 和 -confdir
//   - 把 config.json + confdir/*.json 按 xray 的合并语义合并成单个 config.json
//   - 把 confdir 内 *.json 备份到一个隐藏目录,让重启后 xray 不再读多片配置
//   - 重启 xray(由调用方走的 mode 决定:embedded / external)
//
// 触发场景:从妙妙屋(mmw)迁移到妙妙屋X 时,被妙妙屋管理过的外置 xray 通常用
// `-config FILE -confdir DIR` 多片配置启动;mmwx 主控的 /api/child/inbounds 等接口
// 只读写单个 config 文件,如不合并就会出现"接口改的 client/inbound 丢失"。

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"mmw-agent/internal/discovery"
)

type takeoverExternalXrayResp struct {
	Success      bool   `json:"success"`
	Detected     bool   `json:"detected"`       // 是否检测到正在跑的外置 xray(及配置路径)
	ConfigPath   string `json:"config_path"`    // 合并后写入的 config 路径
	ConfDir      string `json:"conf_dir"`       // 检测到的 confdir(可能为空)
	MergedFiles  int    `json:"merged_files"`   // confdir 下被合并的 *.json 数量
	BackupDir    string `json:"backup_dir"`     // confdir 备份位置(<confdir>/.mmwx-bak-<ts>)
	Restarted    bool   `json:"restarted"`      // xray 是否重启成功
	Message      string `json:"message"`
}

// HandleTakeoverExternalXray 由主控调用,触发"合并 confdir + 重启"动作。
func (h *ManageHandler) HandleTakeoverExternalXray(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	paths := discovery.Discover()
	if paths.ConfigPath == "" && paths.ConfDir == "" {
		writeJSON(w, http.StatusOK, takeoverExternalXrayResp{
			Success:  true,
			Detected: false,
			Message:  "未检测到外置 xray(无运行进程 / 无 systemd unit / 无静态 config)",
		})
		return
	}

	configPath := paths.ConfigPath
	if configPath == "" {
		writeJSON(w, http.StatusOK, takeoverExternalXrayResp{
			Success:  true,
			Detected: true,
			ConfDir:  paths.ConfDir,
			Message:  "只检测到 confdir 没有主 config,跳过合并(罕见情形,请人工确认)",
		})
		return
	}

	mergedCount, backupDir, err := MergeXrayConfdirInto(paths, configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// 重启 xray(embedded 模式走 embedded restart;external 走 systemctl 等)
	restartErr := h.RestartXray()
	resp := takeoverExternalXrayResp{
		Success:     true,
		Detected:    true,
		ConfigPath:  configPath,
		ConfDir:     paths.ConfDir,
		MergedFiles: mergedCount,
		BackupDir:   backupDir,
		Restarted:   restartErr == nil,
	}
	if restartErr != nil {
		resp.Message = fmt.Sprintf("合并完成但重启 xray 失败: %v", restartErr)
	} else {
		resp.Message = fmt.Sprintf("合并 %d 个 conf 文件 → %s,xray 已重启", mergedCount, configPath)
	}
	log.Printf("[TakeoverXray] %s", resp.Message)
	writeJSON(w, http.StatusOK, resp)
}

// MergeXrayConfdirInto reads paths.ConfigPath (must exist), merges every *.json
// inside paths.ConfDir (alphabetical) on top following xray's merge semantics,
// writes the result back to targetPath (may differ from paths.ConfigPath when
// switching to embedded mode and we want the merged config to live at the
// embedded default path), and moves the source confdir/*.json into a hidden
// backup subdir to prevent the next xray restart from re-loading them.
//
// Returns (merged_count, backup_dir, error). No-op (and no error) when ConfDir
// is empty or contains no *.json — in that case base config is still copied to
// targetPath when targetPath != ConfigPath.
func MergeXrayConfdirInto(paths discovery.XrayPaths, targetPath string) (int, string, error) {
	baseRaw, err := os.ReadFile(paths.ConfigPath)
	if err != nil {
		return 0, "", fmt.Errorf("读主 config 失败 %s: %w", paths.ConfigPath, err)
	}
	var base map[string]any
	if err := json.Unmarshal(baseRaw, &base); err != nil {
		return 0, "", fmt.Errorf("解析主 config 失败: %w", err)
	}

	mergedCount := 0
	backupDir := ""
	if paths.ConfDir != "" {
		entries, err := os.ReadDir(paths.ConfDir)
		if err == nil {
			var jsonFiles []string
			for _, e := range entries {
				if e.IsDir() {
					continue
				}
				if !strings.HasSuffix(strings.ToLower(e.Name()), ".json") {
					continue
				}
				jsonFiles = append(jsonFiles, e.Name())
			}
			sort.Strings(jsonFiles)

			for _, name := range jsonFiles {
				p := filepath.Join(paths.ConfDir, name)
				raw, err := os.ReadFile(p)
				if err != nil {
					log.Printf("[MergeXrayConfdir] 读 %s 失败 (continue): %v", p, err)
					continue
				}
				var overlay map[string]any
				if err := json.Unmarshal(raw, &overlay); err != nil {
					log.Printf("[MergeXrayConfdir] 解析 %s 失败 (continue): %v", p, err)
					continue
				}
				base = mergeXrayConfig(base, overlay)
				mergedCount++
			}

			if mergedCount > 0 {
				ts := time.Now().Format("20060102-150405")
				backupDir = filepath.Join(paths.ConfDir, ".mmwx-bak-"+ts)
				if err := os.MkdirAll(backupDir, 0o755); err == nil {
					for _, name := range jsonFiles {
						src := filepath.Join(paths.ConfDir, name)
						dst := filepath.Join(backupDir, name)
						if err := os.Rename(src, dst); err != nil {
							log.Printf("[MergeXrayConfdir] 移动 %s -> %s 失败 (continue): %v", src, dst, err)
						}
					}
				}
			}
		}
	}

	// 写入 targetPath(可能 = ConfigPath 原地写,也可能 != 比如切换到 embedded 默认路径)
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return mergedCount, backupDir, fmt.Errorf("创建目标目录失败 %s: %w", filepath.Dir(targetPath), err)
	}
	mergedJSON, err := json.MarshalIndent(base, "", "  ")
	if err != nil {
		return mergedCount, backupDir, fmt.Errorf("序列化合并 config 失败: %w", err)
	}
	if err := os.WriteFile(targetPath, mergedJSON, 0o644); err != nil {
		return mergedCount, backupDir, fmt.Errorf("写合并 config 失败 %s: %w", targetPath, err)
	}
	return mergedCount, backupDir, nil
}

// mergeXrayConfig 按 xray 的合并语义合并两个 JSON object:
//   - inbounds / outbounds 数组 → 按 tag 去重合并(同 tag overlay 覆盖,无则追加),
//     这跟 xray-core 自己的 confdir merge 一致,保证幂等(同份 overlay 多次合并不重复)
//   - 其他数组字段(如 routing.rules)→ 追加 concat
//   - 对象字段(如 routing / dns)→ 递归 deep merge
//   - 标量字段(如 log.loglevel)→ overlay 覆盖 base
func mergeXrayConfig(base, overlay map[string]any) map[string]any {
	if base == nil {
		base = map[string]any{}
	}
	for k, v := range overlay {
		existing, has := base[k]
		if !has {
			base[k] = v
			continue
		}
		// inbounds / outbounds 按 tag 去重
		if k == "inbounds" || k == "outbounds" {
			if ea, ok := existing.([]any); ok {
				if va, ok := v.([]any); ok {
					base[k] = mergeTaggedArray(ea, va)
					continue
				}
			}
		}
		// 其他 array + array → concat
		if ea, ok := existing.([]any); ok {
			if va, ok := v.([]any); ok {
				base[k] = append(ea, va...)
				continue
			}
		}
		// object + object → recurse
		if em, ok := existing.(map[string]any); ok {
			if vm, ok := v.(map[string]any); ok {
				base[k] = mergeXrayConfig(em, vm)
				continue
			}
		}
		// 其它 → overlay 覆盖
		base[k] = v
	}
	return base
}

// mergeTaggedArray 合并两个 inbound/outbound 数组,按 tag 字段去重:
//   - overlay 里的元素若 tag 已在 base 中存在 → 替换该位置
//   - 否则 → 追加到末尾
//   - 无 tag 字段的元素 → 追加(不视为冲突)
func mergeTaggedArray(base, overlay []any) []any {
	tagIdx := map[string]int{}
	for i, e := range base {
		if m, ok := e.(map[string]any); ok {
			if t, _ := m["tag"].(string); t != "" {
				tagIdx[t] = i
			}
		}
	}
	for _, e := range overlay {
		m, ok := e.(map[string]any)
		if !ok {
			base = append(base, e)
			continue
		}
		t, _ := m["tag"].(string)
		if t == "" {
			base = append(base, e)
			continue
		}
		if i, has := tagIdx[t]; has {
			base[i] = e
		} else {
			tagIdx[t] = len(base)
			base = append(base, e)
		}
	}
	return base
}
