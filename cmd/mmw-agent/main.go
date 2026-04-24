package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"mmw-agent/internal/agent"
	"mmw-agent/internal/config"
	"mmw-agent/internal/constants"
	"mmw-agent/internal/handler"
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
		// 合并环境变量（环境变量优先）
		cfg.Merge(config.FromEnv())
	} else {
		cfg = config.FromEnv()
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid config: %v", err)
	}

	log.Printf("[Main] Starting mmw-agent")
	log.Printf("[Main] Connection mode: %s", cfg.ConnectionMode)
	log.Printf("[Main] Listen port: %s", cfg.ListenPort)
	log.Printf("[Main] Xray servers: %d configured", len(cfg.XrayServers))

	// 创建 agent 客户端
	agentClient := agent.NewClient(cfg)

	// 创建处理器
	apiHandler := handler.NewAPIHandler(agentClient, cfg.Token)
	manageHandler := handler.NewManageHandler(cfg.Token)

	// 注册 HTTP 路由
	mux := http.NewServeMux()
	handler.RegisterChildRoutes(mux, apiHandler, manageHandler)

	// 健康检查
	mux.HandleFunc(constants.PathHealth, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok","mode":"` + string(agentClient.GetCurrentMode()) + `"}`))
	})

	// 创建 HTTP 服务（不设置 WriteTimeout，避免影响 SSE 长连接）
	server := &http.Server{
		Addr:        ":" + cfg.ListenPort,
		Handler:     handler.SilentAuthMiddleware(cfg.Token, mux),
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

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), constants.DefaultShutdownTimeout)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[Main] HTTP server shutdown error: %v", err)
	}

	log.Printf("[Main] Shutdown complete")
}
