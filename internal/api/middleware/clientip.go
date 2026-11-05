package middleware

import (
	"net"
	"net/http"
	"strings"
)

// IPResolver determines the real client IP of a request.
//
// X-Forwarded-For is attacker-controlled unless the request came through a proxy we operate, so
// it is only consulted when the direct peer is in a trusted CIDR. The header is then walked from
// right to left, skipping trusted hops; the first untrusted address is the client.
type IPResolver struct {
	trusted []*net.IPNet
}

// NewIPResolver parses trusted proxy CIDRs (bare IPs are accepted as /32 or /128).
func NewIPResolver(trustedProxies []string) (*IPResolver, error) {
	r := &IPResolver{}
	for _, raw := range trustedProxies {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if !strings.Contains(raw, "/") {
			if ip := net.ParseIP(raw); ip != nil && ip.To4() != nil {
				raw += "/32"
			} else {
				raw += "/128"
			}
		}
		_, n, err := net.ParseCIDR(raw)
		if err != nil {
			return nil, err
		}
		r.trusted = append(r.trusted, n)
	}
	return r, nil
}

func (r *IPResolver) isTrusted(ip net.IP) bool {
	for _, n := range r.trusted {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// ClientIP returns the client address for rate limiting and logging.
func (r *IPResolver) ClientIP(req *http.Request) string {
	peer, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		peer = req.RemoteAddr
	}
	peerIP := net.ParseIP(peer)
	if r == nil || peerIP == nil || !r.isTrusted(peerIP) {
		return peer
	}
	hops := strings.Split(req.Header.Get("X-Forwarded-For"), ",")
	for i := len(hops) - 1; i >= 0; i-- {
		hop := strings.TrimSpace(hops[i])
		ip := net.ParseIP(hop)
		if ip == nil {
			break // malformed header: fall back to the trusted peer
		}
		if !r.isTrusted(ip) {
			return ip.String()
		}
	}
	return peer
}
