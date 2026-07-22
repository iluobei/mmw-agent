package handler

import (
	"net/http"

	"mmw-agent/internal/constants"
)

// 注册子端 API 路由
// warpHandler 可选,为 nil 时不注册 WARP 相关 endpoint(向后兼容老版本部署)。
func RegisterChildRoutes(mux *http.ServeMux, apiHandler *APIHandler, manageHandler *ManageHandler, warpHandler *WarpHandler) {
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
	mux.HandleFunc(constants.PathChildXrayTestCfg, manageHandler.HandleXrayTestConfig)
	mux.HandleFunc(constants.PathChildNginxInstall, manageHandler.HandleNginxInstall)
	mux.HandleFunc(constants.PathChildNginxRemove, manageHandler.HandleNginxRemove)
	mux.HandleFunc(constants.PathChildNginxConfig, manageHandler.HandleNginxConfig)
	mux.HandleFunc(constants.PathChildNginxCfgFile, manageHandler.HandleNginxConfigFiles)
	mux.HandleFunc(constants.PathChildSystemInfo, manageHandler.HandleSystemInfo)
	mux.HandleFunc(constants.PathChildSystemNICs, manageHandler.HandleSystemNICs)
	mux.HandleFunc(constants.PathChildLogs, manageHandler.HandleGetLogs)
	mux.HandleFunc(constants.PathChildLogFiles, manageHandler.HandleLogFiles)
	mux.HandleFunc(constants.PathChildInbounds, manageHandler.HandleInbounds)
	mux.HandleFunc(constants.PathChildOutbounds, manageHandler.HandleOutbounds)
	mux.HandleFunc(constants.PathChildRouting, manageHandler.HandleRouting)
	mux.HandleFunc(constants.PathChildBatchApply, manageHandler.HandleBatchApply)
	mux.HandleFunc(constants.PathChildScan, manageHandler.HandleScan)
	mux.HandleFunc(constants.PathChildCertDeploy, manageHandler.HandleCertDeploy)
	mux.HandleFunc(constants.PathChildNginxSetup, manageHandler.HandleNginxSetupSSL)
	mux.HandleFunc(constants.PathChildNginxServersList, manageHandler.HandleNginxServersList)
	mux.HandleFunc(constants.PathChildDomainProbe, manageHandler.HandleDomainLatencyProbe)
	mux.HandleFunc(constants.PathChildNginxClearStream, manageHandler.HandleClearStreamPort)
	mux.HandleFunc(constants.PathChildValidateSite, manageHandler.HandleValidateSite)
	mux.HandleFunc(constants.PathChildLimiter, manageHandler.HandleLimiter)
	mux.HandleFunc(constants.PathChildSwitchXrayMode, manageHandler.HandleSwitchXrayMode)
	mux.HandleFunc(constants.PathChildSwitchListenPort, manageHandler.HandleSwitchListenPort)
	mux.HandleFunc(constants.PathChildUpdateMasterURL, manageHandler.HandleUpdateMasterURL)
	mux.HandleFunc(constants.PathChildTakeoverXray, manageHandler.HandleTakeoverExternalXray)

	// SSE 流式安装和卸载接口
	mux.HandleFunc(constants.PathChildXrayInstallStream, manageHandler.HandleXrayInstallStream)
	mux.HandleFunc(constants.PathChildXrayRemoveStream, manageHandler.HandleXrayRemoveStream)
	mux.HandleFunc(constants.PathChildNginxInstallSSE, manageHandler.HandleNginxInstallStream)
	mux.HandleFunc(constants.PathChildNginxRemoveSSE, manageHandler.HandleNginxRemoveStream)
	mux.HandleFunc(constants.PathChildAgentUpgradeStream, manageHandler.HandleAgentUpgradeStream)
	mux.HandleFunc(constants.PathChildAgentUninstallStream, manageHandler.HandleAgentUninstallStream)

	// WARP 出站管理 — 老版本部署 warpHandler 为 nil 时跳过(不影响其他功能)
	if warpHandler != nil {
		mux.HandleFunc(constants.PathChildWarpInstall, warpHandler.HandleInstall)
		mux.HandleFunc(constants.PathChildWarpStatus, warpHandler.HandleStatus)
		mux.HandleFunc(constants.PathChildWarpLicense, warpHandler.HandleLicense)
		mux.HandleFunc(constants.PathChildWarpRemove, warpHandler.HandleRemove)
	}
}
