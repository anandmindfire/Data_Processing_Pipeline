// Package auth issues, verifies, lists, and revokes managed API keys.
//
// Keys look like "dpp_<43 base64url chars>" (256 bits of randomness). Only the SHA-256 hash of a
// key is stored; the plaintext is returned once, at creation. Because the secret has full
// entropy, a fast hash is sufficient (no password-style stretching is needed) and lookups by hash
// are index-friendly.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

// KeyPrefix marks managed keys so they can be told apart from the bootstrap admin key and found
// by secret scanners.
const KeyPrefix = "dpp_"

const (
	secretBytes   = 32
	displayLength = 12 // "dpp_" + 8 characters shown in listings
	minTTL        = time.Minute
	// cacheTTL bounds how long a revoked key can keep working on a replica that cached it.
	cacheTTL      = 15 * time.Second
	touchInterval = time.Minute
)

var (
	// ErrInvalidKey means the key is unknown, malformed, revoked, or expired.
	ErrInvalidKey = errors.New("invalid or expired API key")
	// ErrKeyNotFound means no key exists with the given ID.
	ErrKeyNotFound = errors.New("api key not found")
	// ErrTooManyKeys means the active key limit was reached.
	ErrTooManyKeys = errors.New("active api key limit reached; revoke unused keys first")

	validName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9 _.\-]{0,63}$`)
)

// Options configures key lifetimes and limits.
type Options struct {
	DefaultTTL time.Duration
	MaxTTL     time.Duration
	MaxActive  int
}

// ValidationError reports an invalid key request (bad name or lifetime).
type ValidationError struct{ msg string }

func (e *ValidationError) Error() string { return e.msg }

// Service manages API keys on top of a storage.APIKeyStore.
type Service struct {
	store storage.APIKeyStore
	opts  Options
	now   func() time.Time

	mu    sync.Mutex
	cache map[string]cachedKey // secret hash -> key
}

type cachedKey struct {
	key       models.APIKey
	fetchedAt time.Time
	touchedAt time.Time
}

// NewService creates a key service.
func NewService(store storage.APIKeyStore, opts Options) *Service {
	if opts.DefaultTTL <= 0 {
		opts.DefaultTTL = 30 * 24 * time.Hour
	}
	if opts.MaxTTL < opts.DefaultTTL {
		opts.MaxTTL = opts.DefaultTTL
	}
	if opts.MaxActive <= 0 {
		opts.MaxActive = 100
	}
	return &Service{store: store, opts: opts, now: time.Now, cache: make(map[string]cachedKey)}
}

// Issued is returned once at creation and is the only time the secret is visible.
type Issued struct {
	models.APIKey
	Key string `json:"key"`
}

// HashSecret returns the hex SHA-256 digest under which a key is stored.
func HashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// ValidateRequest checks a key name and lifetime and returns the effective TTL (DefaultTTL when
// ttl is zero).
func (s *Service) ValidateRequest(name string, ttl time.Duration) (time.Duration, error) {
	if !validName.MatchString(name) {
		return 0, &ValidationError{"name must be 1-64 characters: letters, digits, space, '_', '-', '.'"}
	}
	if ttl == 0 {
		ttl = s.opts.DefaultTTL
	}
	if ttl < minTTL || ttl > s.opts.MaxTTL {
		return 0, &ValidationError{fmt.Sprintf("expiry must be between %s and %s from now", minTTL, s.opts.MaxTTL)}
	}
	return ttl, nil
}

// Create issues a new key that expires after ttl (DefaultTTL when zero).
func (s *Service) Create(ctx context.Context, name string, ttl time.Duration) (*Issued, error) {
	ttl, err := s.ValidateRequest(strings.TrimSpace(name), ttl)
	if err != nil {
		return nil, err
	}
	keys, err := s.store.ListAPIKeys(ctx)
	if err != nil {
		return nil, err
	}
	now := s.now().UTC()
	active := 0
	for _, k := range keys {
		if k.Valid(now) {
			active++
		}
	}
	if active >= s.opts.MaxActive {
		return nil, ErrTooManyKeys
	}

	secret, err := randomToken(secretBytes)
	if err != nil {
		return nil, err
	}
	secret = KeyPrefix + secret
	id, err := randomToken(9)
	if err != nil {
		return nil, err
	}
	key := models.APIKey{
		ID:        "key_" + id,
		Name:      strings.TrimSpace(name),
		Prefix:    secret[:displayLength],
		CreatedAt: now,
		ExpiresAt: now.Add(ttl),
	}
	if err := s.store.CreateAPIKey(ctx, key, HashSecret(secret)); err != nil {
		return nil, err
	}
	slog.Info("api key created", "key_id", key.ID, "name", key.Name, "expires_at", key.ExpiresAt)
	return &Issued{APIKey: key, Key: secret}, nil
}

// List returns every key (never secrets), newest first.
func (s *Service) List(ctx context.Context) ([]models.APIKey, error) {
	return s.store.ListAPIKeys(ctx)
}

// Revoke disables a key immediately on this replica and within cacheTTL on the others.
func (s *Service) Revoke(ctx context.Context, id string) error {
	found, err := s.store.RevokeAPIKey(ctx, id, s.now().UTC())
	if err != nil {
		return err
	}
	if !found {
		return ErrKeyNotFound
	}
	s.mu.Lock()
	for h, c := range s.cache {
		if c.key.ID == id {
			delete(s.cache, h)
		}
	}
	s.mu.Unlock()
	slog.Info("api key revoked", "key_id", id)
	return nil
}

// Verify checks a presented key and returns the matching key record.
func (s *Service) Verify(ctx context.Context, secret string) (*models.APIKey, error) {
	if !strings.HasPrefix(secret, KeyPrefix) || len(secret) < displayLength {
		return nil, ErrInvalidKey
	}
	hash := HashSecret(secret)
	now := s.now().UTC()

	s.mu.Lock()
	c, cached := s.cache[hash]
	s.mu.Unlock()
	if !cached || now.Sub(c.fetchedAt) > cacheTTL {
		k, err := s.store.FindAPIKeyByHash(ctx, hash)
		if err != nil {
			return nil, err
		}
		if k == nil {
			s.forget(hash)
			return nil, ErrInvalidKey
		}
		c = cachedKey{key: *k, fetchedAt: now, touchedAt: c.touchedAt}
	}
	if !c.key.Valid(now) {
		s.forget(hash)
		return nil, ErrInvalidKey
	}
	if now.Sub(c.touchedAt) >= touchInterval {
		c.touchedAt = now
		if err := s.store.TouchAPIKey(ctx, c.key.ID, now); err != nil {
			slog.Warn("failed recording api key use", "key_id", c.key.ID, "error", err)
		}
	}
	s.mu.Lock()
	s.cache[hash] = c
	s.mu.Unlock()
	key := c.key
	return &key, nil
}

func (s *Service) forget(hash string) {
	s.mu.Lock()
	delete(s.cache, hash)
	s.mu.Unlock()
}

// Purge deletes keys that expired or were revoked before cutoff.
func (s *Service) Purge(ctx context.Context, cutoff time.Time) (int, error) {
	return s.store.DeleteAPIKeysBefore(ctx, cutoff)
}

func randomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate random token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
