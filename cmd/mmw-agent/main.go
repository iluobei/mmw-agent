package main

import (
	"context"
	"flag"
	"log"
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
		configPath := ""
		if len(cfg.XrayServers) > 0 {
			configPath = cfg.XrayServers[0].ConfigPath
		}
		if configPath == "" {
			configPath = constants.DefaultXrayConfigPaths[0]
			log.Printf("[Main] Embedded mode: no xray server discovered, using default config path: %s", configPath)
		}

		// 停止外部 Xray 避免端口冲突
		log.Printf("[Main] Stopping external xray service before embedded start...")
		_ = exec.Command("systemctl", "stop", "xray").Run()
		_ = exec.Command("systemctl", "disable", "xray").Run()

		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			log.Printf("[Main] Embedded mode: config not found, creating minimal config")
			_ = os.MkdirAll(filepath.Dir(configPath), 0755)
			_ = os.WriteFile(configPath, []byte("{}"), 0644)
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
	go func() {
		log.Printf("[Main] HTTP server listening on :%s", cfg.ListenPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
