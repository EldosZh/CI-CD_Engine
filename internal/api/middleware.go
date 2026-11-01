package api

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

func lifecycleMiddleware(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token != "" && r.URL.Path != "/health" {
			given := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || subtle.ConstantTimeCompare([]byte(given), []byte(token)) != 1 {
				write(w, 401, map[string]any{"error": map[string]string{"code": "UNAUTHORIZED", "message": "bearer token required"}})
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
