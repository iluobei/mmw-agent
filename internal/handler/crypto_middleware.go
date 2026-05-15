package handler

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"mmw-agent/internal/constants"
	"mmw-agent/internal/securechan"
)

// CryptoMiddleware 为 Pull 模式提供请求/响应加密。
func CryptoMiddleware(masterPubKey ed25519.PublicKey, next http.Handler) http.Handler {
	if masterPubKey == nil {
		return next
	}

	cache := securechan.NewSessionCache(1 * time.Hour)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := pullExtractToken(r)

		if kxHeader := r.Header.Get("X-Key-Exchange"); kxHeader != "" {
			parts := strings.SplitN(kxHeader, "|", 2)
			if len(parts) != 2 {
				http.Error(w, `{"error":"invalid key exchange"}`, http.StatusBadRequest)
				return
			}

			masterEphPub, err := base64.StdEncoding.DecodeString(parts[0])
			if err != nil || len(masterEphPub) != 32 {
				http.Error(w, `{"error":"invalid master ephemeral key"}`, http.StatusBadRequest)
				return
			}
			sig, err := base64.StdEncoding.DecodeString(parts[1])
			if err != nil {
				http.Error(w, `{"error":"invalid signature"}`, http.StatusBadRequest)
				return
			}

			if !securechan.Verify(masterPubKey, masterEphPub, sig) {
				http.Error(w, `{"error":"signature verification failed"}`, http.StatusForbidden)
				return
			}

			agentPriv, agentPub, err := securechan.GenerateEphemeral()
			if err != nil {
				http.Error(w, `{"error":"key generation failed"}`, http.StatusInternalServerError)
				return
			}

			sharedSecret, err := securechan.ComputeSharedSecret(agentPriv, masterEphPub)
			if err != nil {
				http.Error(w, `{"error":"ECDH failed"}`, http.StatusInternalServerError)
				return
			}

			session, err := securechan.DeriveSession(sharedSecret, agentPub, masterEphPub, false)
			if err != nil {
				http.Error(w, `{"error":"session derivation failed"}`, http.StatusInternalServerError)
				return
			}

			if token != "" {
				cache.Set(token, session)
			}
			w.Header().Set("X-Key-Exchange", base64.StdEncoding.EncodeToString(agentPub))
			log.Printf("[Pull Crypto] Key exchange completed")

			next.ServeHTTP(w, r)
			return
		}

		if r.Header.Get("X-Encrypted") == "1" && token != "" {
			session := cache.Get(token)
			if session == nil {
				http.Error(w, `{"error":"no session, re-negotiate"}`, http.StatusPreconditionFailed)
				return
			}

			body, _ := io.ReadAll(r.Body)
			if len(body) > 0 {
				plaintext, err := session.Decrypt(body)
				if err != nil {
					http.Error(w, `{"error":"decrypt failed"}`, http.StatusBadRequest)
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(plaintext))
				r.ContentLength = int64(len(plaintext))
				r.Header.Set(constants.HeaderContentType, constants.ContentTypeJSON)
			}

			cw := &cryptoResponseWriter{ResponseWriter: w, session: session}
			next.ServeHTTP(cw, r)
			cw.flush()
			return
		}

		next.ServeHTTP(w, r)
	})
}

type cryptoResponseWriter struct {
	http.ResponseWriter
	session    *securechan.Session
	buf        bytes.Buffer
	statusCode int
}

func (cw *cryptoResponseWriter) WriteHeader(code int) {
	cw.statusCode = code
}

func (cw *cryptoResponseWriter) Write(data []byte) (int, error) {
	return cw.buf.Write(data)
}

func (cw *cryptoResponseWriter) flush() {
	encrypted, err := cw.session.Encrypt(cw.buf.Bytes())
	if err != nil {
		if cw.statusCode > 0 {
			cw.ResponseWriter.WriteHeader(cw.statusCode)
		}
		cw.ResponseWriter.Write(cw.buf.Bytes())
		return
	}
	cw.ResponseWriter.Header().Set("X-Encrypted", "1")
	cw.ResponseWriter.Header().Set("Content-Type", "application/octet-stream")
	if cw.statusCode > 0 {
		cw.ResponseWriter.WriteHeader(cw.statusCode)
	}
	cw.ResponseWriter.Write(encrypted)
}

func pullExtractToken(r *http.Request) string {
	auth := r.Header.Get(constants.HeaderAuthorization)
	if after, ok := strings.CutPrefix(auth, constants.BearerPrefix); ok {
		return after
	}
	if token := r.Header.Get(constants.HeaderMMRemoteToken); token != "" {
		return token
	}
	return ""
}
