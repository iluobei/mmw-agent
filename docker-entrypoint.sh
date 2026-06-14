#!/bin/sh
set -e

# DOCKER=1 由 Dockerfile ENV 注入,这里再写一遍兜底(用户用 docker run -e 覆盖时不丢)
export DOCKER=1

# ───────────────────────────────────────────────────────────────────
# host 网络强制检查
#
# 为什么必须 host 网络?
#  - xray 入站端口是用户在主控前端动态加的,bridge 模式得跟着改 -p 映射
#  - agent 自己监听端口给主控反向 WS/HTTP 连接,bridge 又得多映射一个
#  - reality / WSS nginx 在 443 / 80,跟宿主可能冲突,bridge 模式下 -p 80/443 也很难管
#
# 检测原理:bridge 模式典型特征是容器内 /proc/net/dev 有 eth0(veth),且没有宿主的网卡
# 比如 ens* / enp* / wlan*。host 模式则相反 — 共享宿主网络栈,有所有宿主网卡。
# 设 MMWX_REQUIRE_HOST_NETWORK=0 可绕过(不推荐,debug 用)。
# ───────────────────────────────────────────────────────────────────
if [ "${MMWX_REQUIRE_HOST_NETWORK:-1}" = "1" ]; then
    # bridge 模式典型特征:默认路由网关是 docker 默认桥 (172.17.x.x / 172.18.x.x),
    # host 模式下默认网关是宿主真实网关(IP 段任意,但绝不会是 172.17/16)。
    # 仅用 grep 看接口名不靠谱(WSL2 / 一些 VPS 也只有 eth0)。
    DEFAULT_GW=$(awk '$2=="00000000" {print $3; exit}' /proc/net/route 2>/dev/null || echo "")
    # /proc/net/route 的网关是小端 16 进制,172.17.x.x = 0x11AC = "0011AC" 在结尾
    case "$DEFAULT_GW" in
        *0011AC*|*0012AC*)
            # 172.17.x.x / 172.18.x.x — docker 默认桥
            echo ""
            echo "❌ mmw-agent 必须用 host 网络模式启动!"
            echo ""
            echo "  当前检测到 docker bridge 默认网关(172.17.x.x / 172.18.x.x)。"
            echo ""
            echo "  正确启动方式:"
            echo "    docker run --network host ..."
            echo "    或 docker-compose.yml 加: network_mode: host"
            echo ""
            echo "  原因:xray 入站端口是用户在主控动态加的,bridge 模式得跟着改 -p 映射;"
            echo "        agent 监听给主控反向连接,bridge 也得多映射一个端口。host 直通宿主网络栈最省心。"
            echo ""
            echo "  调试时绕过(不推荐):  -e MMWX_REQUIRE_HOST_NETWORK=0"
            echo ""
            exit 1
            ;;
    esac
fi

# ───────────────────────────────────────────────────────────────────
# geoip.dat / geosite.dat
#
# embedded xray 启动前必须有这两个文件(main.go L137 ensureGeoData 同步阻塞),
# 没有 routing 解析 geoip:cn 会 panic。镜像第一次起 / volume 挂 /usr/local/bin 时
# 可能没文件,这里下到 binary 同目录(agent 用 os.Executable() 找 dat)。
# ───────────────────────────────────────────────────────────────────
GEO_DIR="/usr/local/bin"
for dat in geoip.dat geosite.dat; do
    if [ ! -f "$GEO_DIR/$dat" ]; then
        echo "[entrypoint] downloading $dat ..."
        # GitHub 直链;墙内服务器可能慢但 agent 一般装在能访问 GitHub 的境外 VPS,可接受
        wget -q -O "$GEO_DIR/$dat" \
            "https://github.com/v2fly/domain-list-community/releases/latest/download/$dat" \
            2>/dev/null || \
        wget -q -O "$GEO_DIR/$dat" \
            "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/$dat" \
            2>/dev/null || {
            echo "[entrypoint] WARN: $dat 下载失败,embedded xray 路由可能 panic"
        }
    fi
done

# 启动 agent
exec "$@"
