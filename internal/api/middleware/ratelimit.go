package middleware

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

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
	stop    chan struct{}
	done    chan struct{}
}

// NewRateLimiter initializes a RateLimiter with rps and burst limits.
func NewRateLimiter(rps float64, burst int) *RateLimiter {
	rl := &RateLimiter{
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
			cutoff := time.Now().Add(-10 * time.Minute)
			for ip, b := range rl.clients {
				if b.lastRefill.Before(cutoff) {
					delete(rl.clients, ip)
				}
			}
			rl.mu.Unlock()
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
		ip := extractIP(r)
		if !rl.allow(ip) {
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

// extractIP extracts the client IP address from the X-Forwarded-For header or RemoteAddr.
func extractIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
