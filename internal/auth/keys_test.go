package auth

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

func newTestService(t *testing.T, opts Options) (*Service, *storage.MemoryRepository, *time.Time) {
	t.Helper()
	store := storage.NewMemoryRepository()
	svc := NewService(store, opts)
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return clock }
	return svc, store, &clock
}

func TestCreateAndVerify(t *testing.T) {
	svc, store, _ := newTestService(t, Options{DefaultTTL: time.Hour, MaxTTL: 24 * time.Hour})
	ctx := context.Background()

	issued, err := svc.Create(ctx, "ci-bot", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(issued.Key, KeyPrefix) || len(issued.Key) != len(KeyPrefix)+43 {
		t.Fatalf("unexpected key format %q", issued.Key)
	}
	if issued.Prefix != issued.Key[:displayLength] || issued.ExpiresAt.Sub(issued.CreatedAt) != time.Hour {
		t.Fatalf("unexpected metadata: %+v", issued.APIKey)
	}
	// Only the hash is stored.
	if k, _ := store.FindAPIKeyByHash(ctx, issued.Key); k != nil {
		t.Fatal("plaintext secret must not be usable as a lookup key")
	}
	if k, _ := store.FindAPIKeyByHash(ctx, HashSecret(issued.Key)); k == nil || k.ID != issued.ID {
		t.Fatal("key must be stored under its SHA-256 hash")
	}

	got, err := svc.Verify(ctx, issued.Key)
	if err != nil || got.ID != issued.ID {
		t.Fatalf("Verify = %+v %v", got, err)
	}
	if k, _ := store.FindAPIKeyByHash(ctx, HashSecret(issued.Key)); k.LastUsedAt == nil {
		t.Fatal("expected last_used_at to be recorded")
	}
	for _, bad := range []string{"", "dpp_", "dpp_wrong-key-value-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "not-a-managed-key"} {
		if _, err := svc.Verify(ctx, bad); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("Verify(%q) should fail, got %v", bad, err)
		}
	}
}

func TestExpiryAndRevocation(t *testing.T) {
	svc, _, clock := newTestService(t, Options{DefaultTTL: time.Hour, MaxTTL: 24 * time.Hour})
	ctx := context.Background()

	short, _ := svc.Create(ctx, "short", 10*time.Minute)
	long, _ := svc.Create(ctx, "long", 2*time.Hour)
	if _, err := svc.Verify(ctx, short.Key); err != nil {
		t.Fatal(err)
	}

	*clock = clock.Add(11 * time.Minute) // past expiry, even though the key is cached
	if _, err := svc.Verify(ctx, short.Key); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("expired key must be rejected, got %v", err)
	}
	if _, err := svc.Verify(ctx, long.Key); err != nil {
		t.Fatalf("unexpired key must still work: %v", err)
	}

	if err := svc.Revoke(ctx, long.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Verify(ctx, long.Key); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("revoked key must be rejected immediately on this replica, got %v", err)
	}
	if err := svc.Revoke(ctx, "key_missing"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	list, _ := svc.List(ctx)
	statuses := map[string]string{}
	for _, k := range list {
		statuses[k.Name] = k.Status(*clock)
	}
	if statuses["short"] != "expired" || statuses["long"] != "revoked" {
		t.Fatalf("unexpected statuses %v", statuses)
	}

	*clock = clock.Add(48 * time.Hour)
	if n, err := svc.Purge(ctx, clock.Add(-time.Hour)); err != nil || n != 2 {
		t.Fatalf("Purge = %d %v", n, err)
	}
}

func TestRevocationReachesOtherReplicasAfterCache(t *testing.T) {
	store := storage.NewMemoryRepository()
	a, b := NewService(store, Options{}), NewService(store, Options{})
	clock := time.Now()
	a.now = func() time.Time { return clock }
	b.now = func() time.Time { return clock }
	ctx := context.Background()

	issued, _ := a.Create(ctx, "shared", time.Hour)
	if _, err := b.Verify(ctx, issued.Key); err != nil {
		t.Fatal(err)
	}
	_ = a.Revoke(ctx, issued.ID)
	clock = clock.Add(cacheTTL + time.Second)
	if _, err := b.Verify(ctx, issued.Key); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("replica B must see the revocation once its cache entry expires, got %v", err)
	}
}

func TestCreateValidationAndLimits(t *testing.T) {
	svc, _, _ := newTestService(t, Options{DefaultTTL: time.Hour, MaxTTL: 24 * time.Hour, MaxActive: 2})
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		ttl  time.Duration
	}{
		{"", time.Hour},
		{"bad/name", time.Hour},
		{strings.Repeat("x", 65), time.Hour},
		{"ok", 30 * time.Second},
		{"ok", 25 * time.Hour},
	} {
		var v *ValidationError
		if _, err := svc.Create(ctx, tc.name, tc.ttl); !errors.As(err, &v) {
			t.Fatalf("Create(%q, %s) should be a validation error, got %v", tc.name, tc.ttl, err)
		}
	}
	_, _ = svc.Create(ctx, "one", 0)
	_, _ = svc.Create(ctx, "two", 0)
	if _, err := svc.Create(ctx, "three", 0); !errors.Is(err, ErrTooManyKeys) {
		t.Fatalf("expected ErrTooManyKeys, got %v", err)
	}
}
