package ingestion

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// ErrSSRFBlocked is returned when an HTTP source URL targets a restricted or private destination.
var ErrSSRFBlocked = errors.New("SSRF protection: destination address is restricted or private")

func isRestrictedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return true
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
