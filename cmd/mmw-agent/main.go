package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"

	"mmw-agent/internal/agent"
	"mmw-agent/internal/config"
	"mmw-agent/internal/constants"
	"mmw-agent/internal/discovery"
	"mmw-agent/internal/embedded"
	"mmw-agent/internal/handler"
	"mmw-agent/internal/securechan"
	"mmw-agent/internal/selfupdate"
	"mmw-agent/internal/util"
	"mmw-agent/internal/warp"
)

// setupLogging 把日志输出切到 lumberjack 文件 + stdout。
// 大小轮转:单文件 50MB,最多 2 个文件(当前 + 1 备份),超出自动删最旧。
func setupLogging(logPath string) {
	if logPath == "" {
		logPath = config.DefaultLogPath
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0755); err != nil {
		log.Printf("[Main] 创建日志目录失败,继续仅用 stdout: %v", err)
		return
	}
	lj := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    50, // MB
		MaxBackups: 1,  // 当前 + 1 备份 = 最多 2 个文件
		MaxAge:     0,
		Compress:   false,
	}
	log.SetOutput(io.MultiWriter(os.Stdout, lj))
	// 兜底巡检(主轮转由 lumberjack 负责,这里 10 分钟扫一次)
	go cleanupLogsLoop(filepath.Dir(logPath))
}

// setupMemoryLimit 给进程设内存软上限(GOMEMLIMIT 等价),让 GC 在接近上限时更激进回收。
// 优先级:用户已设的 GOMEMLIMIT > MMWX_LOG... 不,> MMWX_MEM_LIMIT_MB > embedded 模式按系统内存 75% 自动设。
// external 模式 agent 内存很小,不自动设(避免无谓的 GC 压力)。
func setupMemoryLimit(embedded bool) {
	// 用户已通过 GOMEMLIMIT 环境变量显式设置(runtime 启动时已生效)→ 尊重,不覆盖。
	if debug.SetMemoryLimit(-1) != math.MaxInt64 {
		return
	}
	if v := os.Getenv("MMWX_MEM_LIMIT_MB"); v != "" {
		if mb, err := strconv.Atoi(v); err == nil && mb > 0 {
			debug.SetMemoryLimit(int64(mb) << 20)
			log.Printf("[Main] 内存软上限 %d MiB (MMWX_MEM_LIMIT_MB)", mb)
			return
		}
	}
	if !embedded {
		return
	}
	total := readMemTotalBytes()
	if total > 0 {
		limit := total / 4 * 3 // 系统内存的 75%,留余量给系统/其他进程
		debug.SetMemoryLimit(limit)
		log.Printf("[Main] embedded 内存软上限 %d MiB (系统 %d MiB ×75%%)", limit>>20, total>>20)
	}
}

// readMemTotalBytes 从 /proc/meminfo 读 MemTotal(kB),返回字节数;失败返回 0。
func readMemTotalBytes() int64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	return parseMemTotalBytes(string(data))
}

// parseMemTotalBytes 解析 /proc/meminfo 文本,返回 MemTotal 的字节数(kB×1024);无 MemTotal 返回 0。
func parseMemTotalBytes(meminfo string) int64 {
	for _, line := range strings.Split(meminfo, "\n") {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line) // ["MemTotal:", "1024000", "kB"]
		if len(fields) >= 2 {
			if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
				return kb * 1024
			}
		}
		break
	}
	return 0
}

func cleanupLogsLoop(dir string) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	enforceMaxLogFiles(dir, "mmw-agent", 2)
	for range ticker.C {
		enforceMaxLogFiles(dir, "mmw-agent", 2)
	}
}

// enforceMaxLogFiles 保留 dir 下以 prefix 开头的最新 keep 个文件,其余按修改时间从旧到新删除。
func enforceMaxLogFiles(dir, prefix string, keep int) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	type ft struct {
		name string
		mod  time.Time
	}
	var files []ft
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, ft{e.Name(), info.ModTime()})
	}
	if len(files) <= keep {
		return
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mod.After(files[j].mod) })
	for _, f := range files[keep:] {
		_ = os.Remove(filepath.Join(dir, f.name))
	}
}

func main() {
	// 隐藏子命令:校验升级二进制签名,供升级脚本在替换前调用(用内嵌公钥验签)。
	// 必须在 flag.Parse 之前拦截。
	if len(os.Args) >= 2 && os.Args[1] == "__verify-update" {
		if len(os.Args) != 4 {
			fmt.Fprintln(os.Stderr, "usage: mmw-agent __verify-update <binary> <sig>")
			os.Exit(2)
		}
		if err := selfupdate.VerifyFile(os.Args[2], os.Args[3]); err != nil {
			fmt.Fprintln(os.Stderr, "verify failed:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

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

	// 日志真实落地到文件 + 保留 stdout(供 systemd journald / 容器查看)。
	setupLogging(cfg.LogPath)

	// embedded 模式 agent 内联 xray 承载所有代理连接,设内存软上限让 GC 更早回收、抑制 RSS 暴涨。
	setupMemoryLimit(cfg.XrayMode == "embedded")

	log.Printf("[Main] Starting mmw-agent")
	log.Printf("[Main] Log file: %s", cfg.LogPath)
	log.Printf("[Main] Connection mode: %s", cfg.ConnectionMode)
	log.Printf("[Main] Xray mode: %s", cfg.XrayMode)
	log.Printf("[Main] Listen port: %s", cfg.ListenPort)
	log.Printf("[Main] Xray servers: %d configured", len(cfg.XrayServers))
	log.Printf("[Main] Restart method: %s", cfg.RestartMethod)

	// 创建处理器
	manageHandler := handler.NewManageHandler(cfg.Token, cfg.RestartMethod, cfg.RestartCommand)
	manageHandler.SetConfigPath(cfgFile)
	manageHandler.SetLogPath(cfg.LogPath) // 供「Agent 日志」页读取 agent 自身日志文件
	manageHandler.SetXrayMode(cfg.XrayMode)

	// WARP 服务 — 状态文件 warp.json 跟 config.yaml 同目录(空 cfgFile 时用当前工作目录)
	warpWorkDir := "."
	if cfgFile != "" {
		warpWorkDir = filepath.Dir(cfgFile)
	}
	warpService := warp.NewService(warpWorkDir)
	warpHandler := handler.NewWarpHandler(cfg.Token, warpService, manageHandler)

	// geoip.dat / geosite.dat 不分 mode 都要准备好 — 主控的 xray test-config 在 external mode
	// 下若 LookPath("xray") 失败(典型: xray 还在 install 流程中)会 fallback 走 xray-core 库
	// 解析,这条路径解析 `geoip:cn` 之类规则时必须能找到 dat 文件,否则 test 报错
	// "open /usr/local/bin/geoip.dat: no such file or directory",主控首次 auto-deploy
	// tunnel 配置直接失败。external mode 异步下载(不阻塞 agent 启动,有就跳过);
	// embedded mode 必须同步 — 内嵌 xray instance.New 会预解析 routing,无 dat 就 panic,
	// 同步那段在下面 embedded 分支里另起调用。
	if cfg.XrayMode != "embedded" {
		go ensureGeoData()
	}

	// 许可证配额授权:主控上次判定本机是否在服务器配额内。超额时主控下发 xray_authorized=0 并由 agent 落盘,
	// 重启时据此决定是否拉起 xray(「重启立即检查」)。nil/未配置 = 首次或默认 → 授权先跑;
	// 连上主控后由 handleConfigUpdate 的 xray_authorized 分支校正到最新值。
	xrayAuthorized := cfg.XrayAuthorized == nil || *cfg.XrayAuthorized
	if !xrayAuthorized {
		log.Printf("[Main] 许可证配额:本机上次被主控判定为超额(xray_authorized=0),启动时不拉起 xray,等待主控重新授权")
	}

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
		// Docker 镜像里没有外部 xray binary 也没有 systemd,跳过 systemctl 调用避免无谓的失败 noise;
		// embedded 模式直接接管,不会有端口冲突。
		if !util.IsDocker() {
			log.Printf("[Main] Stopping external xray service before embedded start...")
			_ = exec.Command("systemctl", "stop", "xray").Run()
			_ = exec.Command("systemctl", "disable", "xray").Run()
		}

		ensureGeoData()
		initXrayConfig(configPath, cfg.StealMode)
		// 补全配置（api、stats、policy、routing等）
		if result := manageHandler.EnsureXrayConfig(); result.Modified {
			log.Printf("[Main] Embedded mode: config auto-completed, added: %v", result.AddedSections)
		}

		// 启动自愈:给端口转发补齐 UDP + full-cone(修历史遗留的 tcp-only 入站 / freedom UseIP 出站)。
		patchTunnelForwardUDPFullcone(configPath)

		if !xrayAuthorized {
			// 超额未授权:准备好配置但不启动 xray。留 embeddedXray=nil,待主控重新授权时
			// 由 StartXray → lazyStartEmbeddedXray 拉起。embedded tunnel 模式下 nginx 接管 443。
			log.Printf("[Main] 许可证配额未授权,跳过 embedded Xray 启动(配置已就绪)")
		} else {
			log.Printf("[Main] Starting embedded Xray with config: %s", configPath)
			embeddedXray = embedded.New(configPath)
			if err := embeddedXray.Start(); err != nil {
				log.Printf("[Main] Warning: embedded Xray failed to start (will retry via lazy-start): %v", err)
				embeddedXray = nil
			} else {
				manageHandler.SetEmbeddedXray(embeddedXray)
			}
		}
	}

	// 外部模式：启动时自动检测并补全 xray 配置
	if embeddedXray == nil && !xrayAuthorized {
		// 超额未授权(external 模式,或 embedded 未授权跳过启动):停掉 xray,等待主控重新授权。
		// tunnel 模式下 StopXray 会先让 nginx 接管 443。
		log.Printf("[Main] 许可证配额未授权,停止 xray 服务")
		manageHandler.StopXray()
	} else if embeddedXray == nil {
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
	// 注入 WARP 状态查询回调,让 auth/heartbeat 上报 warp_installed
	agentClient.SetWarpStatusFn(warpService.IsInstalled)
	manageHandler.OnEmbeddedXrayStart(func(ex *embedded.EmbeddedXray) {
		agentClient.SetEmbeddedXray(ex)
	})
	// 注入许可证配额授权回调:主控下发 xray_authorized 变化时,授权→启 xray、超额→停 xray。
	// 复用 ManageHandler 的 StopXray/StartXray(含 embedded/external + tunnel 模式 nginx 443 让路)。
	agentClient.SetXrayAuthHandler(func(authorized bool) {
		if authorized {
			log.Printf("[Main] 许可证配额:已授权,启动 xray")
			if err := manageHandler.StartXray(); err != nil {
				log.Printf("[Main] 许可证配额授权后启动 xray 失败: %v", err)
			}
		} else {
			log.Printf("[Main] 许可证配额:超额,停止 xray")
			manageHandler.StopXray()
		}
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
	handler.RegisterChildRoutes(mux, apiHandler, manageHandler, warpHandler)

	// 注入 mux 给 client,让 WS RPC 路径(master 反向调用)能复用同一份 /api/child/* handler
	// 实例。共享 mux 意味着 handler 任何后续 bug fix 都同时覆盖 HTTP 和 WS RPC 路径。
	// agent auth payload 会据此上报 capabilities.rpc=true。
	agentClient.SetRPCMux(mux)

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
	log.Printf("[Main] HTTP server listening on :%s", cfg.ListenPort)
	// gate 接管 Serve;初始保持监听(默认行为)。开启端口隐身后,WS 连上会(延迟)关闭监听、
	// WS 断开立即重开。
	gate := newListenGate(server, httpLn)

	// 端口隐身:WS 可用期间关闭入站监听,让外部扫描探测不到 agent;主控指令此时走 WS 反向 RPC,
	// 入站端口仅 WS 不可用时的 HTTP/pull 回退需要。钩子在 Start 之前注入,避免 WS 抢先连上时漏挂。
	if cfg.HidePortOnWS != nil && *cfg.HidePortOnWS {
		agentClient.SetListenGateHooks(gate.onWSConnected, gate.onWSDisconnected)
		log.Printf("[Main] WS-stealth enabled: inbound :%s closes while WebSocket is connected", cfg.ListenPort)
	}

	// 启动 agent 客户端(在监听 + 钩子就绪之后)
	agentClient.Start(ctx)

	// 等待退出信号
	sig := <-sigCh
	log.Printf("[Main] Received signal %v, shutting down...", sig)

	// 优雅退出
	cancel()
	gate.stop() // 停掉待执行的端口开关,避免退出过程中又重开监听
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

// listenCloseGrace 是 WS 连上后延迟关闭入站监听的宽限期:给在途的 HTTP 回退请求收尾,
// 并避免 WS 抖动时端口被频繁开关。
const listenCloseGrace = 5 * time.Second

// listenGate 运行时管理 agent 入站监听的开关。
//
// 思路:主控对 agent 的反向指令在 WS 可用时全部走 WS 反向 RPC,入站端口(默认 23889)只是
// WS 不可用时的 HTTP/pull 回退通道。于是 WS 连上后(延迟)关闭监听以隐藏 agent,WS 断开后
// 立即重开以保住回退。
//
// 实现上只开关 net.Listener、复用同一个 *http.Server(从不对 Server 调 Shutdown/Close,
// 故可对新 Listener 反复 Serve)。所有状态由 mu 保护。
type listenGate struct {
	mu      sync.Mutex
	server  *http.Server
	addr    string       // 监听地址,如 ":23889"(已含 boot 阶段冲突切换后的最终端口)
	ln      net.Listener // 当前监听;nil 表示已关闭
	timer   *time.Timer  // 待执行的延迟关闭
	stopped bool         // 进程退出中,忽略后续开关
}

// newListenGate 接管已 bind 的 listener 并开始 Serve(初始保持监听)。
func newListenGate(server *http.Server, ln net.Listener) *listenGate {
	g := &listenGate{server: server, addr: server.Addr, ln: ln}
	g.serve(ln)
	return g
}

func (g *listenGate) serve(ln net.Listener) {
	go func() {
		// ln.Close() 触发的 net.ErrClosed、Server.Shutdown 触发的 ErrServerClosed 都是预期收尾,不报错。
		if err := g.server.Serve(ln); err != nil &&
			!errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			log.Printf("[Main] HTTP server error: %v", err)
		}
	}()
}

// onWSConnected:WS 鉴权成功,宽限期后关闭入站监听(隐藏端口)。
func (g *listenGate) onWSConnected() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return
	}
	if g.timer != nil {
		g.timer.Stop()
	}
	g.timer = time.AfterFunc(listenCloseGrace, func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.timer = nil
		if g.stopped || g.ln == nil {
			return
		}
		log.Printf("[Main] WebSocket connected; closing inbound listener on %s (stealth)", g.addr)
		_ = g.ln.Close()
		g.ln = nil
	})
}

// onWSDisconnected:WS 断开,立即重开监听,保证主控 HTTP/pull 回退可达。
func (g *listenGate) onWSDisconnected() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return
	}
	if g.timer != nil {
		g.timer.Stop()
		g.timer = nil
	}
	if g.ln != nil {
		return // 已在监听
	}
	var ln net.Listener
	var err error
	for i := 0; i < 5; i++ {
		if ln, err = net.Listen("tcp", g.addr); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		log.Printf("[Main] WARN: reopen inbound listener on %s failed: %v (HTTP/pull fallback unavailable until next WS drop)", g.addr, err)
		return
	}
	g.ln = ln
	log.Printf("[Main] WebSocket down; inbound listener reopened on %s (fallback)", g.addr)
	g.serve(ln)
}

// stop 在进程退出时调用:停掉待执行的关闭,并禁止后续开关(避免退出过程中又重开监听)。
func (g *listenGate) stop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stopped = true
	if g.timer != nil {
		g.timer.Stop()
		g.timer = nil
	}
}

// geoMirrorTemplates 下载 v2ray-rules-dat geoip/geosite 的镜像列表(按 %s = 文件名拼接)。
// 顺序选择 v6 友好 → 国内可达 → GitHub 原始,任一成功即停。
//
// 背景:GitHub Release 重定向到 objects.githubusercontent.com,该域名只有 A 记录(无 AAAA),
// 纯 v6 机器(如澳门 Debee mo-d.2ha.me)直接 connect: network is unreachable → geoip.dat
// 拿不到 → 嵌入式 xray 启动 routing 引用 geoip:cn 加载失败 → server 整个起不来。
//
// jsdelivr 全球 CDN 同时提供 v4/v6;ghproxy 同款。GitHub 原始保留兜底(国内有 v4 时可达)。
var geoMirrorTemplates = []string{
	"https://cdn.jsdelivr.net/gh/Loyalsoldier/v2ray-rules-dat@release/%s",
	"https://mirror.ghproxy.com/https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/%s",
	"https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/%s",
}

func ensureGeoData() {
	exePath, err := os.Executable()
	if err != nil {
		log.Printf("[Main] Cannot determine executable path for geodata: %v", err)
		return
	}
	dir := filepath.Dir(exePath)

	for _, name := range []string{"geoip.dat", "geosite.dat"} {
		dest := filepath.Join(dir, name)
		if _, err := os.Stat(dest); err == nil {
			continue
		}
		log.Printf("[Main] Downloading %s (trying %d mirrors)...", name, len(geoMirrorTemplates))
		var lastErr error
		downloaded := false
		for i, tpl := range geoMirrorTemplates {
			url := fmt.Sprintf(tpl, name)
			if err := downloadFile(dest, url); err != nil {
				lastErr = err
				log.Printf("[Main] mirror %d/%d (%s) failed: %v", i+1, len(geoMirrorTemplates), shortHost(url), err)
				continue
			}
			log.Printf("[Main] Downloaded %s from mirror %d/%d (%s)", name, i+1, len(geoMirrorTemplates), shortHost(url))
			downloaded = true
			break
		}
		if !downloaded {
			log.Printf("[Main] Failed to download %s from all mirrors (last error: %v) — embedded xray routing rules using geoip:xxx will fail to load",
				name, lastErr)
		}
	}
}

// shortHost 从完整 URL 提取 host 部分给日志短一点
func shortHost(rawurl string) string {
	if i := strings.Index(rawurl, "://"); i >= 0 {
		rest := rawurl[i+3:]
		if j := strings.Index(rest, "/"); j >= 0 {
			return rest[:j]
		}
		return rest
	}
	return rawurl
}

func downloadFile(dest, url string) error {
	// 给单镜像下载加 timeout — 之前裸 http.Get 无超时,纯 v6 机器 dial v4 会卡到 TCP 内核 timeout (~75s),
	// 多镜像 fallback 失去意义。30s 足够正常下完几 MB 的 geoip.dat。
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
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

// patchTunnelForwardUDPFullcone 启动自愈:扫描 xray config,给端口转发补齐 UDP + full-cone。
//   - 转发入站(protocol tunnel / dokodemo-door,排除基础设施 api / tunnel-in):settings.network 缺 udp 就补成 "tcp,udp"。
//     (api 是 gRPC 命令通道、tunnel-in 是 reality 443 的 TLS 入站,都只能 tcp,跳过。)
//   - 转发出站(protocol freedom,tag 以 "tunnel-" 开头):domainStrategy 若为 UseIP* 则改 AsIs。
//     UseIP 会按包重解析目标 → UDP 退化成对称 NAT;AsIs 不重解析,保住 full-cone。
// 仅在有改动时写回,避免每次启动无谓写盘。
func patchTunnelForwardUDPFullcone(path string) {
	data, err := os.ReadFile(path)
	if err != nil || len(data) <= 4 {
		return
	}
	var config map[string]any
	if err := json.Unmarshal(data, &config); err != nil {
		return
	}
	changed := false

	if inbounds, ok := config["inbounds"].([]any); ok {
		for _, ibAny := range inbounds {
			ib, _ := ibAny.(map[string]any)
			if ib == nil {
				continue
			}
			proto, _ := ib["protocol"].(string)
			if proto != "tunnel" && proto != "dokodemo-door" {
				continue
			}
			if tag, _ := ib["tag"].(string); tag == "api" || tag == "tunnel-in" {
				continue
			}
			settings, _ := ib["settings"].(map[string]any)
			if settings == nil {
				continue
			}
			if net, _ := settings["network"].(string); !strings.Contains(net, "udp") {
				settings["network"] = "tcp,udp"
				changed = true
			}
		}
	}

	if outbounds, ok := config["outbounds"].([]any); ok {
		for _, obAny := range outbounds {
			ob, _ := obAny.(map[string]any)
			if ob == nil {
				continue
			}
			if proto, _ := ob["protocol"].(string); proto != "freedom" {
				continue
			}
			if tag, _ := ob["tag"].(string); !strings.HasPrefix(tag, "tunnel-") {
				continue
			}
			settings, _ := ob["settings"].(map[string]any)
			if settings == nil {
				continue
			}
			if ds, _ := settings["domainStrategy"].(string); strings.HasPrefix(ds, "UseIP") {
				settings["domainStrategy"] = "AsIs"
				changed = true
			}
		}
	}

	if !changed {
		return
	}
	out, err := json.MarshalIndent(config, "", "    ")
	if err != nil {
		return
	}
	if err := os.WriteFile(path, out, 0644); err != nil {
		log.Printf("[Main] tunnel UDP/full-cone 自愈补丁写回失败: %v", err)
		return
	}
	log.Printf("[Main] tunnel UDP/full-cone 自愈补丁:已给端口转发补齐 udp / 修正 UseIP→AsIs")
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
	tplSignatures := make(map[string]bool)
	for _, r := range templateRules {
		if obj, ok := r.(map[string]any); ok {
			if m, _ := obj["marktag"].(string); m != "" {
				tplMarktags[m] = true
			}
		}
		// 内容签名:防止 template 的"无 marktag"基础 rule(tunnel-in→direct / api→api /
		// geoip:private→block 等)每次 agent 重启 + mergeXrayConfig 都被作为"existing 独有"重新追加。
		tplSignatures[routingRuleSignature(r)] = true
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
		if tplSignatures[routingRuleSignature(r)] {
			continue
		}
		rules = append(rules, r)
	}
	// 按优先级 stable 排序:端口转发(outboundTag tunnel-*,优先级0)置顶、tunnel-in→direct(4)沉底,
	// 与运行时 add_rule 一致。否则重启走 mergeRouting 的 append 会把 existing 端口转发排到
	// template 的 tunnel-in→direct 后面,被 xray 顺序短路匹配截胡(端口转发永久失效)。
	sort.SliceStable(rules, func(i, j int) bool {
		ri, _ := rules[i].(map[string]any)
		rj, _ := rules[j].(map[string]any)
		return handler.ClassifyRulePriority(ri) < handler.ClassifyRulePriority(rj)
	})
	merged["rules"] = rules
	return merged
}

// routingRuleSignature 把一条 routing rule 关键字段规范化为字符串签名,用于 mergeRouting 内容去重。
// 集合字段(inboundTag / ip / domain / user / protocol / network)排序后拼接,保证同语义不同顺序也算同一条。
func routingRuleSignature(r any) string {
	obj, _ := r.(map[string]any)
	if obj == nil {
		return ""
	}
	keys := []string{"type", "outboundTag", "balancerTag", "inboundTag", "ip", "domain", "user", "protocol", "network", "source", "port", "sourcePort", "marktag"}
	var parts []string
	for _, k := range keys {
		v, ok := obj[k]
		if !ok || v == nil {
			continue
		}
		switch vv := v.(type) {
		case []any:
			strs := make([]string, 0, len(vv))
			for _, x := range vv {
				strs = append(strs, fmt.Sprint(x))
			}
			sort.Strings(strs)
			parts = append(parts, k+":["+strings.Join(strs, ",")+"]")
		default:
			parts = append(parts, k+":"+fmt.Sprint(v))
		}
	}
	return strings.Join(parts, "|")
}
