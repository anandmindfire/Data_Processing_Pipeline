package ingestion

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

func TestValidatePublicURL(t *testing.T) {
	tests := []struct {
		name      string
		rawURL    string
		expectErr bool
	}{
		{"valid_public_https", "https://api.github.com/users", false},
		{"valid_public_http", "http://example.com/data.json", false},
		{"blocked_scheme_file", "file:///etc/passwd", true},
		{"blocked_scheme_ftp", "ftp://evil.com/data", true},
		{"blocked_credentials", "https://user:pass@example.com/data", true},
		{"blocked_localhost", "http://localhost:8080/metrics", true},
		{"blocked_loopback_ip", "http://127.0.0.1:8080/admin", true},
		{"blocked_private_ip_10", "http://10.0.0.5/api", true},
		{"blocked_private_ip_192", "http://192.168.1.1/setup", true},
		{"blocked_metadata_ip", "http://169.254.169.254/latest/meta-data", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePublicURL(tc.rawURL)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error for URL %q, got nil", tc.rawURL)
				}
				if !errors.Is(err, ErrSSRFBlocked) {
					t.Fatalf("expected ErrSSRFBlocked, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("expected no error for URL %q, got %v", tc.rawURL, err)
			}
		})
	}
}

func TestSafeHTTPClientBlocksPrivateDestinationsAtDialTime(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"a":1}]`))
	}))
	defer srv.Close()

	// The URL pre-check is bypassed on purpose: the dialer itself must refuse loopback, which is
	// what protects against DNS names that resolve (or re-resolve) to private addresses.
	resp, err := NewSafeHTTPClient(2*time.Second, false).Get(srv.URL)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err == nil || !errors.Is(err, ErrSSRFBlocked) {
		t.Fatalf("expected dial-time SSRF block, got %v", err)
	}

	resp, err = NewSafeHTTPClient(2*time.Second, true).Get(srv.URL)
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("skipping live dial test in restricted sandbox: %v", err)
		}
		t.Fatalf("expected private access when explicitly allowed, got %v", err)
	}
	_ = resp.Body.Close()
}

func TestReaderAllowPrivateNetworksOption(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"a":1},{"a":2}]`))
	}))
	defer srv.Close()

	src := models.Source{Type: "http", Name: "local", URL: srv.URL}
	out := make(chan Item, 4)
	if err := NewReader(nil).Read(context.Background(), src, 0, out, nil); !errors.Is(err, ErrSSRFBlocked) {
		t.Fatalf("expected SSRF block by default, got %v", err)
	}
	if err := NewReader(nil, WithPrivateNetworks(true)).Read(context.Background(), src, 0, out, nil); err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("skipping live dial test in restricted sandbox: %v", err)
		}
		t.Fatalf("expected read to succeed with private networks allowed: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 records, got %d", len(out))
	}
}
