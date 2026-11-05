package response

import (
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// APIKeyResponse describes a managed key without its secret.
type APIKeyResponse struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Prefix     string     `json:"prefix"`
	Status     string     `json:"status"` // active, expired, revoked
	CreatedAt  time.Time  `json:"created_at"`
	ExpiresAt  time.Time  `json:"expires_at"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
}

// CreatedAPIKeyResponse is returned once by POST /auth/keys and includes the secret.
type CreatedAPIKeyResponse struct {
	APIKeyResponse
	Key     string `json:"key"`
	Warning string `json:"warning"`
}

// FromAPIKey maps a key to its public representation.
func FromAPIKey(k models.APIKey, now time.Time) APIKeyResponse {
	return APIKeyResponse{
		ID:         k.ID,
		Name:       k.Name,
		Prefix:     k.Prefix,
		Status:     k.Status(now),
		CreatedAt:  k.CreatedAt,
		ExpiresAt:  k.ExpiresAt,
		RevokedAt:  k.RevokedAt,
		LastUsedAt: k.LastUsedAt,
	}
}
