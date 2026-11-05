package models

import "time"

// APIKey is a managed API credential. The secret itself is never stored: only its SHA-256 hash
// (kept by the store, not exposed here) and a short display prefix.
type APIKey struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Prefix     string     `json:"prefix"` // first characters of the secret, for recognising a key
	CreatedAt  time.Time  `json:"created_at"`
	ExpiresAt  time.Time  `json:"expires_at"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
}

// Status reports whether the key is active, expired, or revoked at time now.
func (k APIKey) Status(now time.Time) string {
	switch {
	case k.RevokedAt != nil:
		return "revoked"
	case !now.Before(k.ExpiresAt):
		return "expired"
	default:
		return "active"
	}
}

// Valid reports whether the key may be used at time now.
func (k APIKey) Valid(now time.Time) bool { return k.Status(now) == "active" }
