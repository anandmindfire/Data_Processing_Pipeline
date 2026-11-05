package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
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
	rl := NewRateLimiter(2.0, 2, nil)
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
	if rec.Header().Get("X-XSS-Protection") != "0" {
		t.Fatalf("expected legacy XSS auditor disabled")
	}
	if rec.Header().Get("Referrer-Policy") != "no-referrer" {
		t.Fatalf("expected Referrer-Policy no-referrer")
	}
	if !strings.Contains(rec.Header().Get("Content-Security-Policy"), "default-src 'none'") {
		t.Fatalf("expected locked-down CSP, got %q", rec.Header().Get("Content-Security-Policy"))
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("expected Cache-Control no-store on API responses")
	}
}

func TestAPIKeyAuthMiddleware(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	t.Run("DisabledWhenEmptyKey", func(t *testing.T) {
		handler := APIKeyAuth(nil, AuthOptions{})(okHandler)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 when auth disabled, got %d", rec.Code)
		}
	})

	t.Run("AllowsPublicRoutesWithoutKey", func(t *testing.T) {
		handler := APIKeyAuth([]string{"secret-123"}, AuthOptions{})(okHandler)
		for _, path := range []string{"/healthz", "/readyz", "/docs", "/docs/openapi.json", "/swagger"} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("path %s expected 200, got %d", path, rec.Code)
			}
		}
	})

	t.Run("RejectsMissingKey", func(t *testing.T) {
		handler := APIKeyAuth([]string{"secret-123"}, AuthOptions{})(okHandler)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 for missing key, got %d", rec.Code)
		}
	})

	t.Run("AcceptsValidKeyInHeader", func(t *testing.T) {
		handler := APIKeyAuth([]string{"secret-123"}, AuthOptions{})(okHandler)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
		req.Header.Set("X-API-Key", "secret-123")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 for valid X-API-Key, got %d", rec.Code)
		}
	})

	t.Run("AcceptsValidKeyInBearerAuth", func(t *testing.T) {
		handler := APIKeyAuth([]string{"secret-123"}, AuthOptions{})(okHandler)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
		req.Header.Set("Authorization", "Bearer secret-123")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 for valid Bearer auth, got %d", rec.Code)
		}
	})
}

func TestAPIKeyAuthMetricsAndRotation(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	call := func(h http.Handler, path, key string) int {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if key != "" {
			req.Header.Set("Authorization", "bearer "+key)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	private := APIKeyAuth([]string{"old-key-0123456789", "new-key-0123456789"}, AuthOptions{})(ok)
	if call(private, "/metrics", "") != http.StatusUnauthorized {
		t.Fatal("/metrics must require the API key unless METRICS_PUBLIC is set")
	}
	for _, k := range []string{"old-key-0123456789", "new-key-0123456789"} {
		if call(private, "/api/v1/pipelines", k) != http.StatusOK {
			t.Fatalf("expected key %q to be accepted during rotation", k)
		}
	}
	if call(private, "/docsevil", "") != http.StatusUnauthorized {
		t.Fatal("paths merely prefixed with /docs must not bypass auth")
	}
	public := APIKeyAuth([]string{"old-key-0123456789"}, AuthOptions{MetricsPublic: true})(ok)
	if call(public, "/metrics", "") != http.StatusOK {
		t.Fatal("expected public /metrics when MetricsPublic is set")
	}
}

func TestIPResolverOnlyTrustsConfiguredProxies(t *testing.T) {
	r, err := NewIPResolver([]string{"10.0.0.0/8", "192.168.1.1"})
	if err != nil {
		t.Fatal(err)
	}
	req := func(remote, xff string) *http.Request {
		q := httptest.NewRequest(http.MethodGet, "/", nil)
		q.RemoteAddr = remote
		if xff != "" {
			q.Header.Set("X-Forwarded-For", xff)
		}
		return q
	}
	cases := []struct{ remote, xff, want string }{
		{"203.0.113.9:1234", "1.2.3.4", "203.0.113.9"},             // untrusted peer: header ignored
		{"10.1.2.3:1234", "1.2.3.4", "1.2.3.4"},                    // trusted proxy
		{"10.1.2.3:1234", "6.6.6.6, 1.2.3.4, 10.9.9.9", "1.2.3.4"}, // spoofed left-most entry ignored
		{"192.168.1.1:80", "garbage", "192.168.1.1"},               // malformed header
		{"10.1.2.3:1234", "", "10.1.2.3"},                          // no header
	}
	for _, c := range cases {
		if got := r.ClientIP(req(c.remote, c.xff)); got != c.want {
			t.Errorf("remote=%s xff=%q: got %s want %s", c.remote, c.xff, got, c.want)
		}
	}
	if _, err := NewIPResolver([]string{"not-a-cidr"}); err == nil {
		t.Fatal("expected invalid CIDR error")
	}
}

func TestRateLimiterCannotBeBypassedWithSpoofedHeader(t *testing.T) {
	rl := NewRateLimiter(1, 1, nil)
	defer rl.Close()
	h := rl.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	codes := []int{}
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "203.0.113.9:5000"
		req.Header.Set("X-Forwarded-For", "198.51.100."+strings.Repeat("1", i+1))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		codes = append(codes, rec.Code)
	}
	if codes[1] != http.StatusTooManyRequests || codes[2] != http.StatusTooManyRequests {
		t.Fatalf("spoofed X-Forwarded-For must not create new buckets, got %v", codes)
	}
}

func TestCORSAllowlist(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h := CORS([]string{"https://app.example.com"})(ok)

	pre := httptest.NewRequest(http.MethodOptions, "/api/v1/pipelines", nil)
	pre.Header.Set("Origin", "https://app.example.com")
	pre.Header.Set("Access-Control-Request-Method", "PATCH")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, pre)
	if rec.Code != http.StatusNoContent || rec.Header().Get("Access-Control-Allow-Origin") != "https://app.example.com" ||
		!strings.Contains(rec.Header().Get("Access-Control-Allow-Methods"), "PATCH") {
		t.Fatalf("unexpected preflight response: %d %v", rec.Code, rec.Header())
	}

	evil := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
	evil.Header.Set("Origin", "https://evil.example.com")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, evil)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("unlisted origins must not receive CORS headers")
	}

	rec = httptest.NewRecorder()
	CORS(nil)(ok).ServeHTTP(rec, evil)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" || rec.Header().Get("Vary") != "" {
		t.Fatal("CORS must be disabled when no origins are configured")
	}
}

func TestRequestIDRejectsUnsafeValues(t *testing.T) {
	var seen string
	h := RequestID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { seen = GetRequestID(r.Context()) }))
	for _, in := range []string{"abc-123_X.y", "bad\nlog-injection", strings.Repeat("a", 65)} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-Request-ID", in)
		h.ServeHTTP(httptest.NewRecorder(), req)
		if in == "abc-123_X.y" && seen != in {
			t.Fatalf("valid request ID should propagate, got %q", seen)
		}
		if in != "abc-123_X.y" && (seen == in || len(seen) != 32) {
			t.Fatalf("unsafe request ID %q should be replaced, got %q", in, seen)
		}
	}
}
