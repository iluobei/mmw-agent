// Package selfupdate 校验 agent 自更新下载的二进制签名。
//
// 公钥在编译期写死在 PubKeyB64,对应的 Ed25519 私钥离线保管(GitHub Actions secret /
// 本地未提交的签名脚本),绝不出现在主控或本仓库提交里。因此即便主控被攻破,
// 攻击者也无法签出能通过校验的恶意 agent 二进制 → 堵死"主控被攻破 → 推木马 binary → RCE"。
package selfupdate

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
)

// PubKeyB64 是升级验签公钥(base64 of 32 字节 ed25519 公钥)。
// 用 ed25519 keygen 生成密钥对后,把【公钥】填到这里(公开,可提交);
// 私钥放 GitHub secret + 本地未提交的签名脚本,切勿提交。
// 留空 → VerifyFile 一律失败 → 自更新被拒(fail-safe,不接受未签名二进制)。
//
// 也可在编译期用 -ldflags "-X 'mmw-agent/internal/selfupdate.PubKeyB64=<base64>'" 覆盖。
var PubKeyB64 = "3aGta5gVWH1jVUInTJopAT7xB8soc4A8FgGEgHrVq6k=" // 升级验签公钥(公开,可提交);对应私钥离线保管

// VerifyFile 用内嵌公钥校验 binPath 的 Ed25519 分离签名(sigPath 为原始 64 字节签名)。
func VerifyFile(binPath, sigPath string) error {
	if PubKeyB64 == "" {
		return errors.New("未配置升级验签公钥(selfupdate.PubKeyB64 为空),拒绝自更新")
	}
	pub, err := base64.StdEncoding.DecodeString(PubKeyB64)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("升级验签公钥非法(需 base64 的 32 字节 ed25519 公钥): %v", err)
	}
	bin, err := os.ReadFile(binPath)
	if err != nil {
		return fmt.Errorf("读取待校验二进制失败: %w", err)
	}
	sig, err := os.ReadFile(sigPath)
	if err != nil {
		return fmt.Errorf("读取签名文件失败: %w", err)
	}
	if len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("签名长度非法(需 %d 字节,实际 %d)", ed25519.SignatureSize, len(sig))
	}
	if !ed25519.Verify(ed25519.PublicKey(pub), bin, sig) {
		return errors.New("签名校验失败:二进制与签名不匹配(可能被篡改或来源不可信)")
	}
	return nil
}
