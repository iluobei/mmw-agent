package handler

import (
	"net/http"

	"mmw-agent/internal/constants"
)

// 注册子端 API 路由
func RegisterChildRoutes(mux *http.ServeMux, apiHandler *APIHandler, manageHandler *ManageHandler) {
	// 拉取模式数据接口
	mux.HandleFunc(constants.PathChildTraffic, apiHandler.ServeHTTP)
	mux.HandleFunc(constants.PathChildSpeed, apiHandler.ServeSpeedHTTP)

	// 管理接口
	mux.HandleFunc(constants.PathChildServiceStats, manageHandler.HandleServicesStatus)
	mux.HandleFunc(constants.PathChildServiceCtl, manageHandler.HandleServiceControl)
	mux.HandleFunc(constants.PathChildXrayInstall, manageHandler.HandleXrayInstall)
	mux.HandleFunc(constants.PathChildXrayRemove, manageHandler.HandleXrayRemove)
	mux.HandleFunc(constants.PathChildXrayConfig, manageHandler.HandleXrayConfig)
	mux.HandleFunc(constants.PathChildXraySysCfg, manageHandler.HandleXraySystemConfig)
	mux.HandleFunc(constants.PathChildXrayCfgFiles, manageHandler.HandleXrayConfigFiles)
	mux.HandleFunc(constants.PathChildNginxInstall, manageHandler.HandleNginxInstall)
	mux.HandleFunc(constants.PathChildNginxRemove, manageHandler.HandleNginxRemove)
	mux.HandleFunc(constants.PathChildNginxConfig, manageHandler.HandleNginxConfig)
	mux.HandleFunc(constants.PathChildNginxCfgFile, manageHandler.HandleNginxConfigFiles)
	mux.HandleFunc(constants.PathChildSystemInfo, manageHandler.HandleSystemInfo)
	mux.HandleFunc(constants.PathChildInbounds, manageHandler.HandleInbounds)
	mux.HandleFunc(constants.PathChildOutbounds, manageHandler.HandleOutbounds)
	mux.HandleFunc(constants.PathChildRouting, manageHandler.HandleRouting)
	mux.HandleFunc(constants.PathChildScan, manageHandler.HandleScan)
	mux.HandleFunc(constants.PathChildCertDeploy, manageHandler.HandleCertDeploy)
	mux.HandleFunc(constants.PathChildNginxSetup, manageHandler.HandleNginxSetupSSL)
	mux.HandleFunc(constants.PathChildDomainProbe, manageHandler.HandleDomainLatencyProbe)
	mux.HandleFunc(constants.PathChildNginxClearStream, manageHandler.HandleClearStreamPort)

	// SSE 流式安装和卸载接口
	mux.HandleFunc(constants.PathChildXrayInstallStream, manageHandler.HandleXrayInstallStream)
	mux.HandleFunc(constants.PathChildXrayRemoveStream, manageHandler.HandleXrayRemoveStream)
	mux.HandleFunc(constants.PathChildNginxInstallSSE, manageHandler.HandleNginxInstallStream)
	mux.HandleFunc(constants.PathChildNginxRemoveSSE, manageHandler.HandleNginxRemoveStream)
	mux.HandleFunc(constants.PathChildAgentUpgradeStream, manageHandler.HandleAgentUpgradeStream)
	mux.HandleFunc(constants.PathChildAgentUninstallStream, manageHandler.HandleAgentUninstallStream)
}
