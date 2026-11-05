package middleware

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// maxTrackedClients caps the bucket map so a flood of distinct source addresses cannot exhaust
// memory. When the cap is hit, idle buckets are evicted; if none are idle, new clients are refused.
const maxTrackedClients = 100_000

type clientBucket struct {
	tokens     float64
	lastRefill time.Time
}

// RateLimiter implements an in-memory token bucket rate limiter per client IP.
type RateLimiter struct {
	mu      sync.Mutex
	clients map[string]*clientBucket
	rps     float64
	burst   int
	ips     *IPResolver
	stop    chan struct{}
	done    chan struct{}
}

// NewRateLimiter initializes a RateLimiter with rps and burst limits. ips resolves the client
// address (nil = use the direct peer address only).
func NewRateLimiter(rps float64, burst int, ips *IPResolver) *RateLimiter {
	rl := &RateLimiter{
		ips:     ips,
		clients: make(map[string]*clientBucket),
		rps:     rps,
		burst:   burst,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	// Background cleanup of stale IP entries
	go rl.cleanupLoop()
	return rl
}

// cleanupLoop periodically evicts client token buckets that have been inactive for over 10 minutes.
func (rl *RateLimiter) cleanupLoop() {
	defer close(rl.done)
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-rl.stop:
			return
		case <-ticker.C:
			rl.mu.Lock()
			rl.evictIdleLocked(10 * time.Minute)
			rl.mu.Unlock()
		}
	}
}

// evictIdleLocked removes buckets idle for longer than idle. Callers must hold rl.mu.
func (rl *RateLimiter) evictIdleLocked(idle time.Duration) {
	cutoff := time.Now().Add(-idle)
	for ip, b := range rl.clients {
		if b.lastRefill.Before(cutoff) {
			delete(rl.clients, ip)
		}
	}
}

// Close gracefully stops the cleanup goroutine.
func (rl *RateLimiter) Close() {
	rl.mu.Lock()
	select {
	case <-rl.stop:
	default:
		close(rl.stop)
	}
	rl.mu.Unlock()
	<-rl.done
}

// allow consumes a token for the given client IP, returning true if the request is permitted.
func (rl *RateLimiter) allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	b, exists := rl.clients[ip]
	if !exists {
		if len(rl.clients) >= maxTrackedClients {
			// A full bucket refills in burst/rps seconds; older buckets carry no state worth keeping.
			rl.evictIdleLocked(time.Duration(float64(rl.burst)/rl.rps*float64(time.Second)) + time.Second)
			if len(rl.clients) >= maxTrackedClients {
				return false
			}
		}
		rl.clients[ip] = &clientBucket{
			tokens:     float64(rl.burst) - 1,
			lastRefill: now,
		}
		return true
	}

	elapsed := now.Sub(b.lastRefill).Seconds()
	b.lastRefill = now
	b.tokens += elapsed * rl.rps
	if b.tokens > float64(rl.burst) {
		b.tokens = float64(rl.burst)
	}

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		return true
	}

	return false
}

// Middleware returns an HTTP handler middleware enforcing rate limits.
func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := rl.ips.ClientIP(r)
		if !rl.allow(ip) {
			retry := 1
			if rl.rps > 0 && rl.rps < 1 {
				retry = int(1/rl.rps) + 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(retry))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "rate limit exceeded; please slow down",
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}
