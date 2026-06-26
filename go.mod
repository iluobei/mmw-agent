module mmw-agent

go 1.26

// vision splice 后绕过 RateWriter 的限速问题需要 fork xray-core 加 conn 层 hook,
// 见 fork 中 proxy/vision_limiter_hook.go + proxy/proxy.go 修改两行。
// 后续 upstream rebase 流程:cp 新版本 → re-apply 这俩文件 → go build。
//
// fork 已发布到 https://github.com/iluobei/Xray-core-mmwx (module 名仍是 github.com/xtls/xray-core)。
// 这里用相对路径 replace:本地在同级目录 ../xray-core-vision-limiter;
// CI 在 build 前 git clone 该 fork 到同位置(见 .github/workflows/build.yml)。
// 不能用 git 版本 replace(=> github.com/iluobei/Xray-core-mmwx),因为 fork module 名 != repo 路径会报错。
replace github.com/xtls/xray-core => ../xray-core-vision-limiter

require (
	github.com/gorilla/websocket v1.5.3
	github.com/xtls/xray-core v1.260327.0
	golang.org/x/crypto v0.49.0
	golang.org/x/time v0.14.0
	google.golang.org/grpc v1.79.3
	google.golang.org/protobuf v1.36.11
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/andybalholm/brotli v1.0.6 // indirect
	github.com/apernet/quic-go v0.59.1-0.20260217092621-db4786c77a22 // indirect
	github.com/cloudflare/circl v1.6.3 // indirect
	github.com/ghodss/yaml v1.0.1-0.20220118164431-d8423dcdf344 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/juju/ratelimit v1.0.2 // indirect
	github.com/klauspost/compress v1.17.4 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/miekg/dns v1.1.72 // indirect
	github.com/pelletier/go-toml v1.9.5 // indirect
	github.com/pires/go-proxyproto v0.11.0 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/refraction-networking/utls v1.8.3-0.20260301010127-aa6edf4b11af // indirect
	github.com/sagernet/sing v0.5.1 // indirect
	github.com/sagernet/sing-shadowsocks v0.2.7 // indirect
	github.com/vishvananda/netlink v1.3.1 // indirect
	github.com/vishvananda/netns v0.0.5 // indirect
	github.com/xtls/reality v0.0.0-20260322125925-9234c772ba8f // indirect
	go4.org/netipx v0.0.0-20231129151722-fdeea329fbba // indirect
	golang.org/x/exp v0.0.0-20241210194714-1829a127f884 // indirect
	golang.org/x/mod v0.33.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	golang.org/x/tools v0.42.0 // indirect
	golang.zx2c4.com/wintun v0.0.0-20230126152724-0fa3db229ce2 // indirect
	golang.zx2c4.com/wireguard v0.0.0-20250521234502-f333402bd9cb // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251222181119-0a764e51fe1b // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gvisor.dev/gvisor v0.0.0-20260122175437-89a5d21be8f0 // indirect
	lukechampine.com/blake3 v1.4.1 // indirect
)
