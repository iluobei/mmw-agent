package acme

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-acme/lego/v4/certcrypto"
	"github.com/go-acme/lego/v4/certificate"
	"github.com/go-acme/lego/v4/challenge/http01"
	"github.com/go-acme/lego/v4/lego"
	"github.com/go-acme/lego/v4/registration"
)

// CertResult represents the result of a certificate issuance.
type CertResult struct {
	Domain     string
	CertPath   string
	KeyPath    string
	CertPEM    string
	KeyPEM     string
	IssueDate  time.Time
	ExpiryDate time.Time
}

// CertRequest contains all parameters for a certificate request.
type CertRequest struct {
	Email          string
	Domain         string
	Provider       string
	ChallengeMode  string
	WebrootPath    string
	DNSProvider    string
	DNSCredentials map[string]string
	EABKid         string
	EABHmacKey     string
}

// User implements the acme.User interface for lego.
type User struct {
	Email        string
	Registration *registration.Resource
	key          *ecdsa.PrivateKey
}

func (u *User) GetEmail() string                        { return u.Email }
func (u *User) GetRegistration() *registration.Resource { return u.Registration }
func (u *User) GetPrivateKey() crypto.PrivateKey        { return u.key }

// Client wraps the lego ACME client.
type Client struct {
	certDir    string
	staging    bool
	httpPort   string
	webrootDir string
}

// ClientOption configures the Client.
type ClientOption func(*Client)

func WithCertDir(dir string) ClientOption {
	return func(c *Client) { c.certDir = dir }
}

func WithStaging(staging bool) ClientOption {
	return func(c *Client) { c.staging = staging }
}

func WithHTTPPort(port string) ClientOption {
	return func(c *Client) { c.httpPort = port }
}

func WithWebrootDir(dir string) ClientOption {
	return func(c *Client) { c.webrootDir = dir }
}

// NewClient creates a new ACME client.
func NewClient(opts ...ClientOption) *Client {
	c := &Client{
		certDir:  "/etc/miaomiaowu/certs",
		staging:  false,
		httpPort: ":80",
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// ObtainCertificate requests a new certificate (backward-compatible HTTP-01 only).
func (c *Client) ObtainCertificate(ctx context.Context, email, domain string, useWebroot bool) (*CertResult, error) {
	mode := "standalone"
	if useWebroot {
		mode = "webroot"
	}
	return c.ObtainCertificateV2(ctx, CertRequest{
		Email:         email,
		Domain:        domain,
		Provider:      CALetsEncrypt,
		ChallengeMode: mode,
		WebrootPath:   c.webrootDir,
	})
}

// ObtainCertificateV2 requests a new certificate with full options support.
func (c *Client) ObtainCertificateV2(ctx context.Context, req CertRequest) (*CertResult, error) {
	if req.Email == "" {
		return nil, errors.New("email is required")
	}
	if req.Domain == "" {
		return nil, errors.New("domain is required")
	}

	client, err := c.buildLegoClient(req)
	if err != nil {
		return nil, err
	}

	obtainReq := certificate.ObtainRequest{
		Domains: []string{req.Domain},
		Bundle:  true,
	}

	certificates, err := client.Certificate.Obtain(obtainReq)
	if err != nil {
		return nil, fmt.Errorf("obtain certificate: %w", err)
	}

	return c.processCertResult(req.Domain, certificates.Certificate, certificates.PrivateKey)
}

func (c *Client) buildLegoClient(req CertRequest) (*lego.Client, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate private key: %w", err)
	}

	user := &User{Email: req.Email, key: privateKey}

	config := lego.NewConfig(user)
	provider := req.Provider
	if provider == "" {
		provider = CALetsEncrypt
	}
	config.CADirURL = ResolveCADirectoryURL(provider, c.staging)
	config.Certificate.KeyType = certcrypto.EC256

	client, err := lego.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("create lego client: %w", err)
	}

	switch req.ChallengeMode {
	case "dns":
		if err := c.setupDNSChallenge(client, req); err != nil {
			return nil, err
		}
	case "webroot":
		if err := c.setupWebrootChallenge(client, req); err != nil {
			return nil, err
		}
	default:
		p := http01.NewProviderServer("", c.httpPort)
		if err := client.Challenge.SetHTTP01Provider(p); err != nil {
			return nil, fmt.Errorf("set http01 provider: %w", err)
		}
	}

	regOpts := registration.RegisterOptions{TermsOfServiceAgreed: true}
	if req.EABKid != "" && req.EABHmacKey != "" {
		reg, err := client.Registration.RegisterWithExternalAccountBinding(registration.RegisterEABOptions{
			TermsOfServiceAgreed: true,
			Kid:                  req.EABKid,
			HmacEncoded:         req.EABHmacKey,
		})
		if err != nil {
			return nil, fmt.Errorf("register with EAB: %w", err)
		}
		user.Registration = reg
	} else {
		reg, err := client.Registration.Register(regOpts)
		if err != nil {
			return nil, fmt.Errorf("register with ACME: %w", err)
		}
		user.Registration = reg
	}

	return client, nil
}

func (c *Client) setupDNSChallenge(client *lego.Client, req CertRequest) error {
	if req.DNSProvider == "" {
		return errors.New("dns_provider is required for DNS-01 challenge")
	}

	if len(req.DNSCredentials) > 0 {
		cleanup, err := SetDNSCredentialEnv(req.DNSProvider, req.DNSCredentials)
		if err != nil {
			return fmt.Errorf("set DNS credentials: %w", err)
		}
		defer cleanup()
	}

	provider, err := NewDNSProviderByName(req.DNSProvider)
	if err != nil {
		return fmt.Errorf("create DNS provider %s: %w", req.DNSProvider, err)
	}

	if err := client.Challenge.SetDNS01Provider(provider); err != nil {
		return fmt.Errorf("set DNS-01 provider: %w", err)
	}
	return nil
}

func (c *Client) setupWebrootChallenge(client *lego.Client, req CertRequest) error {
	webrootDir := req.WebrootPath
	if webrootDir == "" {
		webrootDir = c.webrootDir
	}
	if webrootDir == "" {
		return errors.New("webroot_path is required for webroot challenge")
	}
	provider, err := NewWebrootProvider(webrootDir)
	if err != nil {
		return fmt.Errorf("create webroot provider: %w", err)
	}
	if err := client.Challenge.SetHTTP01Provider(provider); err != nil {
		return fmt.Errorf("set webroot provider: %w", err)
	}
	return nil
}

func (c *Client) processCertResult(domain string, certPEMBytes, keyPEMBytes []byte) (*CertResult, error) {
	expiryDate, issueDate, err := parseCertificateDates(certPEMBytes)
	if err != nil {
		return nil, fmt.Errorf("parse certificate: %w", err)
	}

	certPath, keyPath, err := c.saveCertificate(domain, certPEMBytes, keyPEMBytes)
	if err != nil {
		return nil, fmt.Errorf("save certificate: %w", err)
	}

	return &CertResult{
		Domain:     domain,
		CertPath:   certPath,
		KeyPath:    keyPath,
		CertPEM:    string(certPEMBytes),
		KeyPEM:     string(keyPEMBytes),
		IssueDate:  issueDate,
		ExpiryDate: expiryDate,
	}, nil
}

func (c *Client) saveCertificate(domain string, certPEM, keyPEM []byte) (string, string, error) {
	domainDir := filepath.Join(c.certDir, domain)
	if err := os.MkdirAll(domainDir, 0700); err != nil {
		return "", "", fmt.Errorf("create cert directory: %w", err)
	}

	certPath := filepath.Join(domainDir, "fullchain.pem")
	keyPath := filepath.Join(domainDir, "privkey.pem")

	if err := os.WriteFile(certPath, certPEM, 0644); err != nil {
		return "", "", fmt.Errorf("write certificate: %w", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		return "", "", fmt.Errorf("write private key: %w", err)
	}

	return certPath, keyPath, nil
}

func parseCertificateDates(certPEM []byte) (expiryDate, issueDate time.Time, err error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return time.Time{}, time.Time{}, errors.New("failed to decode PEM block")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("parse certificate: %w", err)
	}

	return cert.NotAfter, cert.NotBefore, nil
}

// GetCertDir returns the certificate storage directory.
func (c *Client) GetCertDir() string {
	return c.certDir
}
