package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mmw-agent/internal/agent"
	"mmw-agent/internal/config"
	"mmw-agent/internal/handler"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	configPathShort := flag.String("c", "", "Path to config file (shorthand)")
	flag.Parse()

	// -c takes effect if -config is not set
	cfgFile := *configPath
	if cfgFile == "" {
		cfgFile = *configPathShort
	}
	// Default to config.yaml in working directory
	if cfgFile == "" {
		if _, err := os.Stat("config.yaml"); err == nil {
			cfgFile = "config.yaml"
		}
	}

	// Load configuration
	var cfg *config.Config
	var err error

	if cfgFile != "" {
		cfg, err = config.Load(cfgFile)
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}
		// Merge environment variables (env takes precedence)
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

	// Create agent client
	agentClient := agent.NewClient(cfg)

	// Create handlers
	apiHandler := handler.NewAPIHandler(agentClient, cfg.Token)
	manageHandler := handler.NewManageHandler(cfg.Token)

	// Setup HTTP routes
	mux := http.NewServeMux()

	// Pull mode API
	mux.HandleFunc("/api/child/traffic", apiHandler.ServeHTTP)
	mux.HandleFunc("/api/child/speed", apiHandler.ServeSpeedHTTP)

	// Management API
	mux.HandleFunc("/api/child/services/status", manageHandler.HandleServicesStatus)
	mux.HandleFunc("/api/child/services/control", manageHandler.HandleServiceControl)
	mux.HandleFunc("/api/child/xray/install", manageHandler.HandleXrayInstall)
	mux.HandleFunc("/api/child/xray/remove", manageHandler.HandleXrayRemove)
	mux.HandleFunc("/api/child/xray/config", manageHandler.HandleXrayConfig)
	mux.HandleFunc("/api/child/xray/system-config", manageHandler.HandleXraySystemConfig)
	mux.HandleFunc("/api/child/xray/config-files", manageHandler.HandleXrayConfigFiles)
	mux.HandleFunc("/api/child/nginx/install", manageHandler.HandleNginxInstall)
	mux.HandleFunc("/api/child/nginx/remove", manageHandler.HandleNginxRemove)
	mux.HandleFunc("/api/child/nginx/config", manageHandler.HandleNginxConfig)
	mux.HandleFunc("/api/child/nginx/config-files", manageHandler.HandleNginxConfigFiles)
	mux.HandleFunc("/api/child/system/info", manageHandler.HandleSystemInfo)
	mux.HandleFunc("/api/child/inbounds", manageHandler.HandleInbounds)
	mux.HandleFunc("/api/child/outbounds", manageHandler.HandleOutbounds)
	mux.HandleFunc("/api/child/routing", manageHandler.HandleRouting)
	mux.HandleFunc("/api/child/scan", manageHandler.HandleScan)
	mux.HandleFunc("/api/child/cert/deploy", manageHandler.HandleCertDeploy)

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok","mode":"` + string(agentClient.GetCurrentMode()) + `"}`))
	})

	// Create HTTP server
	server := &http.Server{
		Addr:         ":" + cfg.ListenPort,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start agent client
	agentClient.Start(ctx)

	// Start HTTP server
	go func() {
		log.Printf("[Main] HTTP server listening on :%s", cfg.ListenPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[Main] HTTP server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	sig := <-sigCh
	log.Printf("[Main] Received signal %v, shutting down...", sig)

	// Graceful shutdown
	cancel()
	agentClient.Stop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[Main] HTTP server shutdown error: %v", err)
	}

	log.Printf("[Main] Shutdown complete")
}
