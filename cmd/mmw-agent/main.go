package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"mmw-agent/internal/agent"
	"mmw-agent/internal/config"
	"mmw-agent/internal/constants"
	"mmw-agent/internal/discovery"
	"mmw-agent/internal/embedded"
	"mmw-agent/internal/handler"
	"mmw-agent/internal/securechan"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	configPathShort := flag.String("c", "", "Path to config file (shorthand)")
	flag.Parse()

	// 仅在 -config 未设置时使用 -c
	cfgFile := *configPath
	if cfgFile == "" {
		cfgFile = *configPathShort
	}
	// 默认读取工作目录下的 config.yaml
	if cfgFile == "" {
		if _, err := os.Stat("config.yaml"); err == nil {
			cfgFile = "config.yaml"
		}
	}

	// 加载配置
	var cfg *config.Config
	var err error

	if cfgFile != "" {
		cfg, err = config.Load(cfgFile)
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}
		// 合并环境变量（环境变量优先，只覆盖实际设置的字段）
		cfg.MergeEnv()
	} else {
		cfg = config.FromEnv()
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid config: %v", err)
	}

	log.Printf("[Main] Starting mmw-agent")
	log.Printf("[Main] Connection mode: %s", cfg.ConnectionMode)
	log.Printf("[Main] Xray mode: %s", cfg.XrayMode)
	log.Printf("[Main] Listen port: %s", cfg.ListenPort)
	log.Printf("[Main] Xray servers: %d configured", len(cfg.XrayServers))
	log.Printf("[Main] Restart method: %s", cfg.RestartMethod)

	// 创建处理器
	manageHandler := handler.NewManageHandler(cfg.Token, cfg.RestartMethod, cfg.RestartCommand)
	manageHandler.SetConfigPath(cfgFile)
	manageHandler.SetXrayMode(cfg.XrayMode)

	// 嵌入模式：启动内嵌 Xray 实例
	var embeddedXray *embedded.EmbeddedXray
	if cfg.XrayMode == "embedded" {
		// embedded 模式统一使用 mmwx 标准路径(constants.DefaultXrayConfigPaths[0],
		// 通常是 /usr/local/etc/xray/config.json),不管以前外置 xray 装在哪。
		// 这样跨服务器路径一致,mmwx UI / API 永远操作同一个文件。
		//
		// 注:config.applyDefaults 启动时会 auto-discover 把发现的路径填进 cfg.XrayServers,
		// embedded 模式下要忽略它(那是外置 xray 的路径,不是 mmwx 接管后的目标)。
		configPath := constants.DefaultXrayConfigPaths[0]

		// 探测当前外置 xray 在跑哪个 config 路径 + confdir,合并迁移到 mmwx 标准路径。
		discovered := discovery.Discover()
		if discovered.ConfigPath != "" && discovered.ConfigPath != configPath {
			merged, backup, err := handler.MergeXrayConfdirInto(discovered, configPath)
			if err != nil {
				log.Printf("[Main] WARN: merge external xray config failed: %v", err)
			} else {
				log.Printf("[Main] Embedded mode: imported external xray config from %s (+%d confdir files) into %s; confdir backup: %s",
					discovered.ConfigPath, merged, configPath, backup)
				// 把原外置 config 归档(防止外置 xray 被误启动后又抢端口/与 mmwx 配置漂移)
				_ = os.Rename(discovered.ConfigPath, discovered.ConfigPath+".before-mmwx-"+time.Now().Format("20060102-150405"))
			}
		} else if discovered.ConfigPath == configPath && discovered.ConfDir != "" {
			// 路径相同但有 confdir 多片,原地合并
			merged, backup, err := handler.MergeXrayConfdirInto(discovered, configPath)
			if err != nil {
				log.Printf("[Main] WARN: merge confdir failed: %v", err)
			} else if merged > 0 {
				log.Printf("[Main] Embedded mode: merged %d confdir files into %s (backup: %s)", merged, configPath, backup)
			}
		}

		// 停止外部 Xray 避免端口冲突
		log.Printf("[Main] Stopping external xray service before embedded start...")
		_ = exec.Command("systemctl", "stop", "xray").Run()
		_ = exec.Command("systemctl", "disable", "xray").Run()

		ensureGeoData()
		initXrayConfig(configPath, cfg.StealMode)
		// 补全配置（api、stats、policy、routing等）
		if result := manageHandler.EnsureXrayConfig(); result.Modified {
			log.Printf("[Main] Embedded mode: config auto-completed, added: %v", result.AddedSections)
		}

		log.Printf("[Main] Starting embedded Xray with config: %s", configPath)
		embeddedXray = embedded.New(configPath)
		if err := embeddedXray.Start(); err != nil {
			log.Printf("[Main] Warning: embedded Xray failed to start (will retry via lazy-start): %v", err)
			embeddedXray = nil
		} else {
			manageHandler.SetEmbeddedXray(embeddedXray)
		}
	}

	// 外部模式：启动时自动检测并补全 xray 配置
	if embeddedXray == nil {
		log.Printf("[Main] Running startup xray auto-detection...")
		result := manageHandler.EnsureXrayConfig()
		if result.Modified {
			log.Printf("[Main] Xray config auto-completed on startup, added: %v", result.AddedSections)
			if err := manageHandler.RestartXray(); err != nil {
				log.Printf("[Main] Failed to restart xray after config update: %v", err)
			} else {
				time.Sleep(1 * time.Second)
			}
		} else if result.Error != "" {
			log.Printf("[Main] Startup xray config check: %s", result.Error)
		} else {
			log.Printf("[Main] Xray config OK, no changes needed")
		}
	}

	// 启动时立刻把缺 tag 的 inbound/outbound 补 tag 写回 xray 配置,不依赖 list 端点触发。
	// list 端点里的同名兜底也保留,作 defense-in-depth(配置后续被手改回缺 tag 时仍能修)。
	// 注:放在 EnsureXrayConfig + 可能的 RestartXray 之后,确保 xray 配置已就位、路径稳定。
	manageHandler.PromoteAllTagsOnStartup()

	// 创建 agent 客户端
	agentClient := agent.NewClient(cfg)
	if embeddedXray != nil {
		agentClient.SetEmbeddedXray(embeddedXray)
	}
	manageHandler.OnEmbeddedXrayStart(func(ex *embedded.EmbeddedXray) {
		agentClient.SetEmbeddedXray(ex)
	})
	// lazyStartEmbeddedXray 可能在回调注册前已经执行（EnsureXrayConfig 触发），补偿传递
	if ex := manageHandler.GetEmbeddedXray(); ex != nil && embeddedXray == nil {
		log.Printf("[Main] Propagating lazy-started embedded Xray to agent client")
		agentClient.SetEmbeddedXray(ex)
		embeddedXray = ex
	}

	// 创建 API 处理器
	apiHandler := handler.NewAPIHandler(agentClient, cfg.Token)

	// 注册 HTTP 路由
	mux := http.NewServeMux()
	handler.RegisterChildRoutes(mux, apiHandler, manageHandler)

	// 健康检查
	mux.HandleFunc(constants.PathHealth, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok","mode":"` + string(agentClient.GetCurrentMode()) + `"}`))
	})

	// 解析 Master 公钥用于 Pull 模式加密
	var pullHandler http.Handler = mux
	if cfg.MasterPublicKey != "" {
		if pubKey, err := securechan.ParsePublicKey(cfg.MasterPublicKey); err == nil {
			pullHandler = handler.CryptoMiddleware(pubKey, mux)
			log.Printf("[Main] Pull mode encryption enabled")
		} else {
			log.Printf("[Main] Warning: invalid master_public_key for pull crypto: %v", err)
		}
	}

	// 创建 HTTP 服务（不设置 WriteTimeout，避免影响 SSE 长连接）
	server := &http.Server{
		Addr:        ":" + cfg.ListenPort,
		Handler:     handler.SilentAuthMiddleware(cfg.Token, pullHandler),
		ReadTimeout: constants.DefaultReadTimeout,
	}

	// 配置优雅退出
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 启动 agent 客户端
	agentClient.Start(ctx)

	// 启动 HTTP 服务
	// 先同步 bind,失败立即 fail-fast 退出进程,让 systemd 把服务标 failed
	// (否则 ListenAndServe 失败但 WebSocket 出站还活,会造成 agent HTTP API 死、
	//  主控误以为"在线"无法触达的死锁状态)
	//
	// 端口冲突处理顺序:**先死磕原端口,再考虑切换**(历史 BUG:顺序反了 → 上次实例的
	// orphan / TIME_WAIT 套接字短暂占港 → 立刻切端口并 persist → 用户配的 NAT 端口转发
	// 全部失效 → 主控反向 HTTP 全 502。LXC 案例就是这个。)
	//
	//   1. listenWithRetry 在原端口轮询 12s,期间还会主动 kill 别的 mmw-agent 进程。
	//      80%+ 的"冲突"其实是 systemd 快速重启导致的 socket 没释放 / orphan,这个阶段就解决。
	//   2. 真的拼不下来才切端口 + persist。这种情况通常是另一个真实进程(xray inbound /
	//      用户自建服务等)长期占港,持久切换是合理的。
	httpLn, err := listenWithRetry("tcp", server.Addr, 6, 2*time.Second)
	if err != nil {
		if newPort, ok := resolveListenPortConflict(cfg.ListenPort); ok {
			log.Printf("[Main] Original port %s stuck after 12s retry; switching to %d and persisting", cfg.ListenPort, newPort)
			cfg.ListenPort = fmt.Sprintf("%d", newPort)
			server.Addr = ":" + cfg.ListenPort
			if err := persistListenPort(cfgFile, cfg.ListenPort); err != nil {
				log.Printf("[Main] Warn: failed to persist new listen_port to %s: %v (next restart will re-detect)", cfgFile, err)
			}
			httpLn, err = net.Listen("tcp", server.Addr)
		}
		if err != nil {
			log.Fatalf("[Main] HTTP server bind failed on :%s: %v", cfg.ListenPort, err)
		}
	}
	go func() {
		log.Printf("[Main] HTTP server listening on :%s", cfg.ListenPort)
		if err := server.Serve(httpLn); err != nil && err != http.ErrServerClosed {
			log.Printf("[Main] HTTP server error: %v", err)
		}
	}()

	// 等待退出信号
	sig := <-sigCh
	log.Printf("[Main] Received signal %v, shutting down...", sig)

	// 优雅退出
	cancel()
	agentClient.Stop()
	if embeddedXray != nil {
		if err := embeddedXray.Stop(); err != nil {
			log.Printf("[Main] Embedded Xray stop error: %v", err)
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), constants.DefaultShutdownTimeout)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[Main] HTTP server shutdown error: %v", err)
	}

	log.Printf("[Main] Shutdown complete")
}

func ensureGeoData() {
	exePath, err := os.Executable()
	if err != nil {
		log.Printf("[Main] Cannot determine executable path for geodata: %v", err)
		return
	}
	dir := filepath.Dir(exePath)

	files := map[string]string{
		"geoip.dat":  "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geoip.dat",
		"geosite.dat": "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geosite.dat",
	}

	for name, url := range files {
		dest := filepath.Join(dir, name)
		if _, err := os.Stat(dest); err == nil {
			continue
		}
		log.Printf("[Main] Downloading %s ...", name)
		if err := downloadFile(dest, url); err != nil {
			log.Printf("[Main] Failed to download %s: %v", name, err)
		} else {
			log.Printf("[Main] Downloaded %s to %s", name, dest)
		}
	}
}

func downloadFile(dest, url string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	tmp := dest + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	f.Close()
	return os.Rename(tmp, dest)
}

// listenWithRetry 在 EADDRINUSE 时按指定间隔重试 bind,其它错误立即返回。
// 解决场景:agent 被 systemd 快速重启时,上一实例的 LISTEN socket 偶尔还没被内核回收;
// 直接 fatal 会触发又一轮 5s 间隔的重启,把秒级问题拖成分钟级。
//
// 兜底:若 attempts 一半之后仍 EADDRINUSE,说明不是"内核延迟回收"而是真的有别的进程在占。
// 主动找系统里其它 mmw-agent 进程(老的、systemd 没追踪到的 zombie)并 SIGKILL,避免无限重启循环。
func listenWithRetry(network, addr string, attempts int, delay time.Duration) (net.Listener, error) {
	var lastErr error
	for i := 0; i < attempts; i++ {
		ln, err := net.Listen(network, addr)
		if err == nil {
			if i > 0 {
				log.Printf("[Main] HTTP bind succeeded on attempt %d/%d", i+1, attempts)
			}
			return ln, nil
		}
		lastErr = err
		// 仅在端口被占用错误下重试,其它(权限/无效地址等)直接报错
		if !strings.Contains(strings.ToLower(err.Error()), "address already in use") {
			return nil, err
		}
		log.Printf("[Main] HTTP bind attempt %d/%d failed on %s (will retry in %v): %v", i+1, attempts, addr, delay, err)
		// 第一次失败就立即扫并杀掉别的 mmw-agent 进程 — systemctl restart 没杀干净老 agent
		// 是最常见的占港原因,等到第 4 次再杀让用户等 8s 没必要
		if i == 0 {
			if n := killOtherMmwAgentProcesses(); n > 0 {
				log.Printf("[Main] Killed %d orphan mmw-agent process(es), retrying bind", n)
			}
		}
		time.Sleep(delay)
	}
	return nil, lastErr
}

// resolveListenPortConflict 探测 cfg.ListenPort 是否能 bind。
// 不能就从该端口往上扫,跳过所有 listening socket,找一个真正空闲的端口返回。
// 返回 (newPort, true) 表示发生切换;(_, false) 表示原端口可用。
// 用途:agent 默认 23889,如果用户 xray inbound / 其它进程已经占了同端口,
// 自动避让到 23890+ 而不是死循环 retry。
func resolveListenPortConflict(currentPort string) (int, bool) {
	want, err := strconv.Atoi(strings.TrimSpace(currentPort))
	if err != nil || want < 1024 || want > 65535 {
		return 0, false
	}
	// 先快速试一下当前端口
	if ln, err := net.Listen("tcp", fmt.Sprintf(":%d", want)); err == nil {
		ln.Close()
		return want, false
	}
	// 当前端口被占 → 从 want+1 往上找
	for offset := 1; offset < 100; offset++ {
		candidate := want + offset
		if candidate > 65535 {
			break
		}
		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", candidate))
		if err != nil {
			continue
		}
		ln.Close()
		return candidate, true
	}
	return 0, false
}

// persistListenPort 把新的 listen_port 写回 yaml config 文件(原行替换 / 没有则追加)。
// 跟 management_handler.HandleSwitchListenPort 的写法保持一致,避免再做 yaml 序列化引入未知字段顺序变动。
func persistListenPort(cfgFile, newPort string) error {
	if cfgFile == "" {
		return fmt.Errorf("config file path empty")
	}
	data, err := os.ReadFile(cfgFile)
	if err != nil {
		return err
	}
	lines := strings.Split(string(data), "\n")
	found := false
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "listen_port:") {
			lines[i] = fmt.Sprintf("listen_port: \"%s\"", newPort)
			found = true
			break
		}
	}
	if !found {
		lines = append(lines, fmt.Sprintf("listen_port: \"%s\"", newPort))
	}
	return os.WriteFile(cfgFile, []byte(strings.Join(lines, "\n")), 0644)
}

// killOtherMmwAgentProcesses 扫 /proc,把 exe 指向 mmw-agent 但 PID 不等于自己的进程全部 SIGKILL。
// 处理"老 agent 没死透 / systemd 没追踪到的 zombie"导致新实例无法 bind 端口的情况。
func killOtherMmwAgentProcesses() int {
	self := os.Getpid()
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0
	}
	killed := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pid := 0
		for _, c := range e.Name() {
			if c < '0' || c > '9' {
				pid = 0
				break
			}
			pid = pid*10 + int(c-'0')
		}
		if pid == 0 || pid == self {
			continue
		}
		exe, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
		if err != nil {
			continue
		}
		// exe 可能是 /usr/local/bin/mmw-agent 或 /tmp/mmw-agent 之类,后缀匹配 mmw-agent
		if !strings.HasSuffix(exe, "/mmw-agent") && !strings.Contains(exe, "mmw-agent ") {
			continue
		}
		if err := syscall.Kill(pid, syscall.SIGKILL); err == nil {
			log.Printf("[Main] SIGKILL orphan mmw-agent pid=%d exe=%s", pid, exe)
			killed++
		}
	}
	return killed
}

func initXrayConfig(path string, stealMode string) {
	_ = os.MkdirAll(filepath.Dir(path), 0755)

	switch stealMode {
	case "tunnel", "fallback":
		// 偷自己模式:把模板需要的入站(tunnel-in / api)、出站(direct/block/nginx)、routing 规则
		// 合并到现有配置里 — 模板有的覆盖,模板没有的(用户自建 inbound/outbound/规则)保留。
		// 历史上此处是直接覆盖,导致重启 agent 后用户在主控里建的所有 inbound/出站链/路由规则全部丢失。
		tpl := embedded.TunnelConfigJSON
		if stealMode == "fallback" {
			tpl = embedded.DefaultConfigJSON
		}
		existing, err := os.ReadFile(path)
		if err != nil || len(existing) <= 4 {
			// 没有现有配置 / 现有配置为空,直接写模板
			_ = os.WriteFile(path, []byte(tpl), 0644)
			log.Printf("[Main] Steal-self mode (%s): no existing config, wrote template", stealMode)
			return
		}
		merged, err := mergeXrayConfig(existing, []byte(tpl))
		if err != nil {
			// 合并失败(JSON 解析异常),退化到原"备份+覆盖"行为
			_ = os.Rename(path, path+".backup")
			log.Printf("[Main] Steal-self mode (%s): merge failed (%v), backed up to %s.backup and wrote template", stealMode, err, path)
			_ = os.WriteFile(path, []byte(tpl), 0644)
			return
		}
		// 写之前留一份备份,出问题用户能回滚
		_ = os.WriteFile(path+".backup", existing, 0644)
		if err := os.WriteFile(path, merged, 0644); err != nil {
			log.Printf("[Main] Steal-self mode (%s): write merged config failed: %v", stealMode, err)
			return
		}
		log.Printf("[Main] Steal-self mode (%s): merged template into existing config (backup: %s.backup)", stealMode, path)
	default:
		// 普通模式：配置不存在或为空时写入默认模板
		info, err := os.Stat(path)
		if os.IsNotExist(err) || (err == nil && info.Size() <= 4) {
			_ = os.WriteFile(path, []byte(embedded.DefaultConfigJSON), 0644)
			log.Printf("[Main] Config missing or empty, wrote default template config")
		}
	}
}

// mergeXrayConfig 把 template 合并进 existing,返回合并后 JSON。
// 合并语义:
//   - inbounds / outbounds: 数组按 tag 合并 — 同 tag 用 template 的,template 没有的 tag 保留 existing 的
//   - routing.rules: 数组按 marktag 合并(template 在前) — 同 marktag 用 template 的,
//     template 没有 marktag 的 existing 规则追加在后(顺序保持,因为 xray 路由按顺序匹配)
//   - 其他顶层字段(log/dns/api/stats/policy/metrics 等): template 提供则覆盖,否则保留 existing
func mergeXrayConfig(existing, template []byte) ([]byte, error) {
	var existMap, tplMap map[string]any
	if err := json.Unmarshal(existing, &existMap); err != nil {
		return nil, fmt.Errorf("parse existing config: %w", err)
	}
	if err := json.Unmarshal(template, &tplMap); err != nil {
		return nil, fmt.Errorf("parse template config: %w", err)
	}

	result := make(map[string]any, len(existMap)+len(tplMap))
	for k, v := range existMap {
		result[k] = v
	}

	for k, v := range tplMap {
		switch k {
		case "inbounds":
			result["inbounds"] = mergeTaggedArray(existMap["inbounds"], v)
		case "outbounds":
			result["outbounds"] = mergeTaggedArray(existMap["outbounds"], v)
		case "routing":
			result["routing"] = mergeRouting(existMap["routing"], v)
		default:
			result[k] = v
		}
	}

	return json.MarshalIndent(result, "", "    ")
}

// mergeTaggedArray 合并两个对象数组,按 tag 字段为主键。template 中存在的 tag 覆盖 existing,
// existing 独有的 tag 保留。返回值: template 列表 + existing 中 tag 不在 template 的项。
func mergeTaggedArray(existingRaw, templateRaw any) []any {
	existing, _ := existingRaw.([]any)
	template, _ := templateRaw.([]any)

	tplTags := make(map[string]bool)
	for _, item := range template {
		if obj, ok := item.(map[string]any); ok {
			if tag, _ := obj["tag"].(string); tag != "" {
				tplTags[tag] = true
			}
		}
	}

	merged := make([]any, 0, len(template)+len(existing))
	merged = append(merged, template...)
	for _, item := range existing {
		obj, ok := item.(map[string]any)
		if !ok {
			merged = append(merged, item)
			continue
		}
		tag, _ := obj["tag"].(string)
		if tag != "" && tplTags[tag] {
			continue // template 已提供同 tag 项,跳过
		}
		merged = append(merged, item)
	}
	return merged
}

// mergeRouting 合并 routing 块。顶层字段(domainStrategy 等)以 template 为准;
// rules 数组按 marktag 合并 — 同 marktag 用 template 的,有/无 marktag 的 existing 规则追加在 template 后。
func mergeRouting(existingRaw, templateRaw any) any {
	existing, _ := existingRaw.(map[string]any)
	template, _ := templateRaw.(map[string]any)
	if template == nil {
		return existing
	}
	if existing == nil {
		return template
	}

	merged := make(map[string]any, len(existing)+len(template))
	for k, v := range existing {
		merged[k] = v
	}
	for k, v := range template {
		if k != "rules" {
			merged[k] = v
		}
	}

	existingRules, _ := existing["rules"].([]any)
	templateRules, _ := template["rules"].([]any)

	tplMarktags := make(map[string]bool)
	for _, r := range templateRules {
		if obj, ok := r.(map[string]any); ok {
			if m, _ := obj["marktag"].(string); m != "" {
				tplMarktags[m] = true
			}
		}
	}

	rules := make([]any, 0, len(templateRules)+len(existingRules))
	rules = append(rules, templateRules...)
	for _, r := range existingRules {
		obj, ok := r.(map[string]any)
		if !ok {
			rules = append(rules, r)
			continue
		}
		if m, _ := obj["marktag"].(string); m != "" && tplMarktags[m] {
			continue
		}
		rules = append(rules, r)
	}
	merged["rules"] = rules
	return merged
}
