package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRecoveryMiddleware(t *testing.T) {
	panickingHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("simulated critical crash")
	})

	handler := Recovery(panickingHandler)

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500 from recovery, got %d", rec.Code)
	}
}

func TestRateLimiterMiddleware(t *testing.T) {
	rl := NewRateLimiter(2.0, 2)
	defer rl.Close()
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := rl.Middleware(okHandler)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.RemoteAddr = "192.168.1.50:54321"

	// First two should succeed (burst = 2)
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("call %d expected 200, got %d", i+1, rec.Code)
		}
	}

	// Third immediate request should exceed limit and get 429
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", rec.Code)
	}
}

func TestSecurityHeadersMiddleware(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := SecurityHeaders(okHandler)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("expected X-Content-Type-Options nosniff")
	}
	if rec.Header().Get("X-Frame-Options") != "DENY" {
		t.Fatalf("expected X-Frame-Options DENY")
	}
	if rec.Header().Get("X-XSS-Protection") != "1; mode=block" {
		t.Fatalf("expected X-XSS-Protection 1; mode=block")
	}
	if rec.Header().Get("Referrer-Policy") != "strict-origin-when-cross-origin" {
		t.Fatalf("expected Referrer-Policy strict-origin-when-cross-origin")
	}
}

func TestAPIKeyAuthMiddleware(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	t.Run("DisabledWhenEmptyKey", func(t *testing.T) {
		handler := APIKeyAuth("")(okHandler)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 when auth disabled, got %d", rec.Code)
		}
	})

	t.Run("AllowsPublicRoutesWithoutKey", func(t *testing.T) {
		handler := APIKeyAuth("secret-123")(okHandler)
		for _, path := range []string{"/healthz", "/metrics", "/docs", "/docs/openapi.json", "/swagger"} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("path %s expected 200, got %d", path, rec.Code)
			}
		}
	})

	t.Run("RejectsMissingKey", func(t *testing.T) {
		handler := APIKeyAuth("secret-123")(okHandler)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 for missing key, got %d", rec.Code)
		}
	})

	t.Run("AcceptsValidKeyInHeader", func(t *testing.T) {
		handler := APIKeyAuth("secret-123")(okHandler)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
		req.Header.Set("X-API-Key", "secret-123")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 for valid X-API-Key, got %d", rec.Code)
		}
	})

	t.Run("AcceptsValidKeyInBearerAuth", func(t *testing.T) {
		handler := APIKeyAuth("secret-123")(okHandler)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
		req.Header.Set("Authorization", "Bearer secret-123")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 for valid Bearer auth, got %d", rec.Code)
		}
	})
}
