package handler

import (
	"net/http"
	"strings"

	"mmw-agent/internal/constants"
)

func SilentAuthMiddleware(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !silentAuthenticate(r, token) {
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					conn.Close()
					return
				}
			}
			return
		}
		next.ServeHTTP(w, r)
	})
}

func silentAuthenticate(r *http.Request, token string) bool {
	if r.Header.Get(constants.HeaderUserAgent) != constants.AgentUserAgent {
		return false
	}

	if token == "" {
		return true
	}

	auth := r.Header.Get(constants.HeaderAuthorization)
	if auth == "" {
		auth = r.Header.Get(constants.HeaderMMRemoteToken)
	}
	if auth == "" {
		return false
	}

	if strings.HasPrefix(auth, constants.BearerPrefix) {
		return strings.TrimPrefix(auth, constants.BearerPrefix) == token
	}

	return auth == token
}
