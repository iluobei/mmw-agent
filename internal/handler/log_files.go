package handler

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Agent 日志文件管理:列出本机日志目录下的文件及占用空间,支持单个删除与一键清空。
//
// 与主控侧 internal/handler/logs_files.go 是同一套语义,两条规则同样必须守住:
//
//  1. **路径穿越**:文件名由主控传入。filepath.Base() 剥目录后再拼接,拼完再验一次父目录。
//     agent 以 root 跑,这里放松一点就是任意文件删除。
//
//  2. **活跃文件只能截断,不能删除**:log.SetOutput 挂着 lumberjack,它一直持有当前
//     日志文件的 fd。unlink 之后 agent 会继续往已删除的 inode 写 —— 磁盘不释放、
//     日志页再也看不到新行,直到下次轮转。故活跃文件走 os.Truncate。
//
// 目录范围就是 agent 日志文件所在目录(cfg.LogPath 的 dir),内嵌 xray 的 access log
// 也落在那里,一并纳管。**不碰 journalctl** —— 那是系统日志,不归 agent 删。

type logFileInfo struct {
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	Modified string `json:"modified"`
	Active   bool   `json:"active"`
}

// HandleLogFiles 处理 GET/DELETE /api/child/logs/files。
func (h *ManageHandler) HandleLogFiles(w http.ResponseWriter, r *http.Request) {
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	if h.logPath == "" {
		writeError(w, http.StatusInternalServerError, "agent log path not configured")
		return
	}

	switch r.Method {
	case http.MethodGet:
		files, total, err := h.collectLogFiles()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "read log dir: "+err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true, "files": files, "total_size": total, "dir": filepath.Dir(h.logPath),
		})
	case http.MethodDelete:
		h.handleDeleteLogFiles(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// activeLogNames 返回"正在被写入、只能截断"的文件名集合。
// agent 自身日志始终在列;内嵌模式下 xray access log 也由本进程持续写入。
func (h *ManageHandler) activeLogNames() map[string]bool {
	active := map[string]bool{filepath.Base(h.logPath): true}
	if h.xrayAccessLogPath != "" {
		active[filepath.Base(h.xrayAccessLogPath)] = true
	}
	return active
}

func (h *ManageHandler) collectLogFiles() ([]logFileInfo, int64, error) {
	dir := filepath.Dir(h.logPath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []logFileInfo{}, 0, nil
		}
		return nil, 0, err
	}
	active := h.activeLogNames()
	out := make([]logFileInfo, 0, len(entries))
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, ierr := e.Info()
		if ierr != nil {
			continue // 刚被轮转删掉
		}
		total += info.Size()
		out = append(out, logFileInfo{
			Name:     e.Name(),
			Size:     info.Size(),
			Modified: info.ModTime().Format("2006-01-02 15:04:05"),
			Active:   active[e.Name()],
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Active != out[j].Active {
			return out[i].Active
		}
		return out[i].Modified > out[j].Modified
	})
	return out, total, nil
}

func (h *ManageHandler) handleDeleteLogFiles(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("all") == "1" {
		files, _, err := h.collectLogFiles()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		removed, freed := 0, int64(0)
		for _, f := range files {
			if perr := h.purgeLogFile(f.Name); perr != nil {
				continue // 单个失败不中断整轮
			}
			removed++
			freed += f.Size
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"success": true, "removed": removed, "freed": freed})
		return
	}

	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		writeError(w, http.StatusBadRequest, "name 不能为空")
		return
	}
	if err := h.purgeLogFile(name); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

// purgeLogFile 清掉一个日志文件:活跃文件截断,其余删除。
func (h *ManageHandler) purgeLogFile(name string) error {
	dir := filepath.Dir(h.logPath)
	path, err := safeLogFilePath(dir, name)
	if err != nil {
		return err
	}
	if h.activeLogNames()[filepath.Base(path)] {
		return os.Truncate(path, 0)
	}
	if rerr := os.Remove(path); rerr != nil && !os.IsNotExist(rerr) {
		return rerr
	}
	return nil
}

// safeLogFilePath 把传入的文件名解析成 dir 下的真实路径,拒绝一切带路径的写法。
//
// 刻意**拒绝**而不是 filepath.Base() 静默改写:Base("../xray.json") 得到 "xray.json",
// 落点仍在日志目录内(穿不出去),但"请求删 A、实际删 B 还返回成功"本身就不该发生。
// 合法日志名来自列表接口,是纯文件名;带路径的输入一定是手工构造的,直接打回。
func safeLogFilePath(dir, name string) (string, error) {
	raw := strings.TrimSpace(name)
	if raw == "" || raw == "." || raw == ".." ||
		strings.ContainsAny(raw, `/\`) || strings.Contains(raw, "..") {
		return "", errors.New("非法的日志文件名")
	}
	path := filepath.Join(dir, raw)
	if filepath.Dir(path) != filepath.Clean(dir) {
		return "", errors.New("非法的日志文件名")
	}
	return path, nil
}
