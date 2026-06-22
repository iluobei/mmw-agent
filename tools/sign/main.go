// 命令 sign:用 Ed25519 私钥对文件做分离签名,输出 <file>.sig(原始 64 字节签名)。
//
// 私钥来自环境变量 AGENT_UPDATE_PRIVATE_KEY(base64 of 64 字节 ed25519 私钥):
//   - CI:用 GitHub Actions secret 注入该 env。
//   - 本地:用未提交的脚本注入。私钥绝不提交。
//
// 用法:AGENT_UPDATE_PRIVATE_KEY=<base64> go run . path/to/binary [more...]
//
// 独立 go.mod(仅 stdlib),不依赖主模块的 xray-core replace,CI/本地任意环境可直接 go run。
package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"os"
)

func main() {
	keyB64 := os.Getenv("AGENT_UPDATE_PRIVATE_KEY")
	if keyB64 == "" {
		fmt.Fprintln(os.Stderr, "AGENT_UPDATE_PRIVATE_KEY 未设置")
		os.Exit(1)
	}
	key, err := base64.StdEncoding.DecodeString(keyB64)
	if err != nil || len(key) != ed25519.PrivateKeySize {
		fmt.Fprintf(os.Stderr, "私钥非法(需 base64 的 %d 字节 ed25519 私钥): %v\n", ed25519.PrivateKeySize, err)
		os.Exit(1)
	}
	priv := ed25519.PrivateKey(key)

	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: sign <file>...")
		os.Exit(2)
	}
	for _, f := range os.Args[1:] {
		data, err := os.ReadFile(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "读取 %s 失败: %v\n", f, err)
			os.Exit(1)
		}
		sig := ed25519.Sign(priv, data)
		out := f + ".sig"
		if err := os.WriteFile(out, sig, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "写 %s 失败: %v\n", out, err)
			os.Exit(1)
		}
		fmt.Printf("signed %s -> %s\n", f, out)
	}
}
