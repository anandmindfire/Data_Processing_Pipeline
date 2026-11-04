package ingestion

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"
)

const maxRedirects = 5

// extraRestrictedNets covers ranges not reported by net.IP helpers.
var extraRestrictedNets = mustParseCIDRs(
	"0.0.0.0/8",     // "this" network
	"100.64.0.0/10", // carrier-grade NAT (also used by some cloud metadata services)
	"192.0.0.0/24",  // IETF protocol assignments
	"198.18.0.0/15", // benchmarking
	"fc00::/7",      // IPv6 unique local (also covered by IsPrivate; kept explicit)
)

func mustParseCIDRs(cidrs ...string) []*net.IPNet {
	out := make([]*net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			panic(err)
		}
		out = append(out, n)
	}
	return out
}

// ErrSSRFBlocked is returned when an HTTP source URL targets a restricted or private destination.
var ErrSSRFBlocked = errors.New("SSRF protection: destination address is restricted or private")

func isRestrictedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return true
	}
	for _, n := range extraRestrictedNets {
		if n.Contains(ip) {
			return true
		}
	}
	// AWS / GCP instance metadata endpoint (169.254.169.254)
	if ip.String() == "169.254.169.254" {
		return true
	}
	return false
}

func validateURLStructure(u *url.URL) error {
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("%w: unsupported scheme %q", ErrSSRFBlocked, u.Scheme)
	}
	if u.User != nil {
		return fmt.Errorf("%w: user credentials in URL are prohibited", ErrSSRFBlocked)
	}
	return nil
}

func validateDestinationHost(host string) error {
	if host == "" || strings.EqualFold(host, "localhost") {
		return fmt.Errorf("%w: localhost is prohibited", ErrSSRFBlocked)
	}

	if ip := net.ParseIP(host); ip != nil {
		if isRestrictedIP(ip) {
			return fmt.Errorf("%w: IP %s is restricted", ErrSSRFBlocked, ip.String())
		}
		return nil
	}

	if ips, err := net.LookupIP(host); err == nil {
		for _, ip := range ips {
			if isRestrictedIP(ip) {
				return fmt.Errorf("%w: resolved IP %s is restricted", ErrSSRFBlocked, ip.String())
			}
		}
	}
	return nil
}

// ValidatePublicURL checks that a URL uses http/https, contains no credentials, and does not target loopback/private IPs.
func ValidatePublicURL(rawURL string) error {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	if err := validateURLStructure(u); err != nil {
		return err
	}
	return validateDestinationHost(u.Hostname())
}

// NewSafeHTTPClient returns an HTTP client for fetching untrusted source URLs.
//
// Unless allowPrivate is set, the destination IP is checked at dial time — after DNS resolution,
// on every connection and every redirect hop — which closes the DNS-rebinding gap left by checking
// the hostname up front. Environment proxies are ignored so requests cannot be routed via a proxy
// on the private network, and redirects are limited and re-validated.
func NewSafeHTTPClient(timeout time.Duration, allowPrivate bool) *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	if !allowPrivate {
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return fmt.Errorf("%w: invalid dial address %q", ErrSSRFBlocked, address)
			}
			if ip := net.ParseIP(host); ip == nil || isRestrictedIP(ip) {
				return fmt.Errorf("%w: connection to %s is not allowed", ErrSSRFBlocked, host)
			}
			return nil
		}
	}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, addr)
		},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          50,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				return fmt.Errorf("stopped after %d redirects", maxRedirects)
			}
			if err := validateURLStructure(req.URL); err != nil {
				return err
			}
			if !allowPrivate {
				return validateDestinationHost(req.URL.Hostname())
			}
			return nil
		},
	}
}
