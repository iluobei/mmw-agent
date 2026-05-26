package main

import (
	"context"
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
	httpLn, err := net.Listen("tcp", server.Addr)
	if err != nil {
		log.Fatalf("[Main] HTTP server bind failed on :%s: %v", cfg.ListenPort, err)
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

func initXrayConfig(path string, stealMode string) {
	_ = os.MkdirAll(filepath.Dir(path), 0755)

	switch stealMode {
	case "tunnel", "fallback":
		// 偷自己模式：备份原配置后用模板覆盖
		if _, err := os.Stat(path); err == nil {
			_ = os.Rename(path, path+".backup")
			log.Printf("[Main] Backed up existing config to %s.backup", path)
		}
		tpl := embedded.TunnelConfigJSON
		if stealMode == "fallback" {
			tpl = embedded.DefaultConfigJSON
		}
		_ = os.WriteFile(path, []byte(tpl), 0644)
		log.Printf("[Main] Steal-self mode (%s): wrote template config", stealMode)
	default:
		// 普通模式：配置不存在或为空时写入默认模板
		info, err := os.Stat(path)
		if os.IsNotExist(err) || (err == nil && info.Size() <= 4) {
			_ = os.WriteFile(path, []byte(embedded.DefaultConfigJSON), 0644)
			log.Printf("[Main] Config missing or empty, wrote default template config")
		}
	}
}
