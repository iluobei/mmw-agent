#!/usr/bin/env bash
# 手动升级 mmw-agent 到 GitHub release(默认 latest,可指定版本如 v0.1.4)。
#
# 适用场景:UI "升级"按钮卡住、agent 进程没换、需要绕过卡死 handler 强制刷新。
#
# 用法:
#   bash upgrade-agent.sh              # 升级到 GitHub latest
#   bash upgrade-agent.sh v0.1.4       # 升级到指定 tag
#
# 兼容:
#   - systemd (Debian/Ubuntu/CentOS 等) — systemctl restart mmw-agent
#   - OpenRC (Alpine LXC 等)            — rc-service mmw-agent restart
#   - 都不在则用 supervise-daemon / 裸 nohup 启动(打印提示由用户接管)
#
# 失败兜底:
#   - 下载失败 → 退出,不动现有 binary
#   - 替换前自动备份到 /usr/local/bin/mmw-agent.bak-<timestamp>,启动失败可手动回滚
#
set -euo pipefail

REPO="iluobei/mmw-agent"
BIN="/usr/local/bin/mmw-agent"
TARGET="${1:-latest}"

err() { echo "[ERROR] $*" >&2; exit 1; }
log() { echo "[$(date +%H:%M:%S)] $*"; }

# 必须 root(写 /usr/local/bin + 控制服务)
[ "$(id -u)" = 0 ] || err "请用 root 运行"

# 1. 探测架构
ARCH=$(uname -m)
case $ARCH in
    x86_64)        ARCH_NAME="amd64" ;;
    aarch64|arm64) ARCH_NAME="arm64" ;;
    *) err "不支持的架构: $ARCH" ;;
esac
log "架构: $ARCH_NAME"

# 2. 解析目标版本 path(URL 前缀由镜像链各自接上)
if [ "$TARGET" = "latest" ]; then
    PATH_SUFFIX="releases/latest/download/mmw-agent-linux-${ARCH_NAME}"
    log "目标: GitHub latest"
else
    # 允许带或不带 v 前缀
    case "$TARGET" in v*) TAG="$TARGET" ;; *) TAG="v$TARGET" ;; esac
    PATH_SUFFIX="releases/download/${TAG}/mmw-agent-linux-${ARCH_NAME}"
    log "目标: $TAG"
fi

# 3. 下载到临时位置(--max-time 防止网络卡死无限等)
# 镜像链 — 纯 v6 机器直连 github 会"network is unreachable"(release binary 重定向到无 AAAA 的
# objects.githubusercontent.com)。ghproxy/gh-proxy 提供 v4+v6 双栈反代,放前面优先命中。
MIRRORS=(
    "https://mirror.ghproxy.com/https://github.com/${REPO}/${PATH_SUFFIX}"
    "https://gh-proxy.com/https://github.com/${REPO}/${PATH_SUFFIX}"
    "https://github.com/${REPO}/${PATH_SUFFIX}"
)
TMP="$(mktemp /tmp/mmw-agent-new.XXXXXX)"
trap 'rm -f "$TMP" "$TMP.sig"' EXIT
download_ok=0
for URL in "${MIRRORS[@]}"; do
    log "下载 $URL ..."
    if command -v curl >/dev/null 2>&1; then
        if curl -fsSL --connect-timeout 10 --max-time 180 -o "$TMP" "$URL"; then
            download_ok=1; break
        fi
    elif command -v wget >/dev/null 2>&1; then
        if wget -q --connect-timeout=10 --read-timeout=180 -O "$TMP" "$URL"; then
            download_ok=1; break
        fi
    else
        err "没有 curl/wget,无法下载"
    fi
    log "  → 该镜像失败,尝试下一个..."
done
[ "$download_ok" = "1" ] || err "所有镜像均下载失败(GitHub + ghproxy + gh-proxy 全部不可达)"
SIZE=$(du -h "$TMP" | cut -f1)
NEW_MD5=$(md5sum "$TMP" | awk '{print $1}')
log "下载完成: $SIZE, md5=$NEW_MD5"

# 3b. 签名校验:下载同名 .sig,用【已装】agent 的内嵌公钥验签(私钥离线,主控/本仓库都没有)。
#     - rc=0  通过 → 继续
#     - rc=1  新版 agent 明确判定签名不匹配 → 中止(防被篡改/MITM 的二进制)
#     - 其它  当前是旧版 agent(不支持 __verify-update)或拿不到 .sig → 警告后继续(过渡期兼容)
SIG="$TMP.sig"
sig_ok=0
for URL in "${MIRRORS[@]}"; do
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL --connect-timeout 10 --max-time 60 -o "$SIG" "${URL}.sig" && { sig_ok=1; break; }
    elif command -v wget >/dev/null 2>&1; then
        wget -q --connect-timeout=10 --read-timeout=60 -O "$SIG" "${URL}.sig" && { sig_ok=1; break; }
    fi
done
if [ "$sig_ok" = 1 ] && [ -x "$BIN" ] && command -v timeout >/dev/null 2>&1; then
    log "校验签名..."
    set +e
    VOUT=$(timeout 15 "$BIN" __verify-update "$TMP" "$SIG" 2>&1); VRC=$?
    set -e
    if [ "$VRC" = 0 ]; then
        log "✅ 签名校验通过"
    elif [ "$VRC" = 1 ]; then
        err "签名校验失败(二进制与签名不匹配,拒绝升级): $VOUT"
    else
        log "[WARN] 无法验签(rc=$VRC,可能当前为旧版 agent 不支持),按原流程继续"
    fi
else
    log "[WARN] 未获取到 .sig 或环境不支持,跳过验签"
fi

# 4. 与现有 binary 对比;一样就不动
if [ -f "$BIN" ]; then
    OLD_MD5=$(md5sum "$BIN" | awk '{print $1}')
    if [ "$OLD_MD5" = "$NEW_MD5" ]; then
        log "现有 binary 已是该版本 (md5=$NEW_MD5),无需替换"
        exit 0
    fi
    BAK="${BIN}.bak-$(date +%s)"
    cp "$BIN" "$BAK"
    log "已备份: $BAK (md5=$OLD_MD5)"
fi

# 5. 原子替换(避免 "text file busy" — 旧进程占着 inode 不能直接 cp 覆盖)
chmod +x "$TMP"
mv -f "$TMP" "$BIN"
trap - EXIT
log "已替换 $BIN"

# 6. 重启服务 — 顺序探测,谁活跃用谁
restarted=0
if [ -d /run/systemd/system ] && command -v systemctl >/dev/null 2>&1 \
   && systemctl list-unit-files mmw-agent.service >/dev/null 2>&1; then
    log "systemd 模式: systemctl restart mmw-agent"
    systemctl restart mmw-agent
    restarted=1
elif command -v rc-service >/dev/null 2>&1 \
     && rc-service --exists mmw-agent 2>/dev/null; then
    log "OpenRC 模式: rc-service mmw-agent restart"
    rc-service mmw-agent restart
    restarted=1
elif pgrep -f "/usr/local/bin/mmw-agent" >/dev/null 2>&1; then
    # 裸 nohup 模式 — kill 老进程,新 binary 需要用户原命令再启
    log "[WARN] 检测到非 systemd/OpenRC 模式 mmw-agent 进程,本脚本不自动重启"
    log "[WARN] 请你手动:pkill -f /usr/local/bin/mmw-agent && nohup /usr/local/bin/mmw-agent -c <config> &"
else
    log "[WARN] 未检测到 mmw-agent 进程或服务,二进制已替换但需要手动启动"
fi

# 7. 验证
sleep 3
if [ $restarted -eq 1 ]; then
    if pgrep -f "/usr/local/bin/mmw-agent" >/dev/null 2>&1; then
        log "✅ 升级完成,agent 正在运行"
    else
        log "[ERROR] agent 进程未起来,查看 journalctl -u mmw-agent / /var/log/mmw-agent.log 排查"
        log "[ERROR] 回滚命令: mv $BAK $BIN && systemctl restart mmw-agent  # 或 rc-service mmw-agent restart"
        exit 1
    fi
fi

log "done"
