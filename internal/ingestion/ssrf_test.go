package ingestion

import (
	"errors"
	"testing"
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
