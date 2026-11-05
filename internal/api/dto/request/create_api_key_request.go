package request

import (
	"errors"
	"fmt"
	"time"
)

// CreateAPIKeyRequest is the body of POST /api/v1/auth/keys. Set at most one of ExpiresIn
// (a Go duration such as "720h") or ExpiresAt (RFC 3339); neither uses the server default.
type CreateAPIKeyRequest struct {
	Name      string     `json:"name"`
	ExpiresIn string     `json:"expires_in,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

// TTL resolves the requested lifetime relative to now (0 = use the server default).
func (r CreateAPIKeyRequest) TTL(now time.Time) (time.Duration, error) {
	switch {
	case r.ExpiresIn != "" && r.ExpiresAt != nil:
		return 0, errors.New("set either expires_in or expires_at, not both")
	case r.ExpiresIn != "":
		d, err := time.ParseDuration(r.ExpiresIn)
		if err != nil || d <= 0 {
			return 0, fmt.Errorf("expires_in must be a positive duration such as \"720h\" (got %q)", r.ExpiresIn)
		}
		return d, nil
	case r.ExpiresAt != nil:
		d := r.ExpiresAt.Sub(now)
		if d <= 0 {
			return 0, errors.New("expires_at must be in the future")
		}
		return d, nil
	}
	return 0, nil
}
