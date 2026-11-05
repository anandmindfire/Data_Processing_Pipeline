package bootstrap

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/handlers"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/middleware"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/routes"
)

// BuildRouter constructs the HTTP router.
//
// Middleware order matters: the request ID is assigned first so every later log line (including
// panic recovery) carries it; rate limiting runs before authentication so API keys cannot be
// brute-forced at full speed; the request timeout wraps only the API routes.
func BuildRouter(deps *Dependencies) http.Handler {
	cfg := deps.Config
	ips := deps.IPs
	if ips == nil {
		var err error
		if ips, err = middleware.NewIPResolver(cfg.TrustedProxies); err != nil {
			slog.Error("invalid TRUSTED_PROXIES; ignoring X-Forwarded-For", "error", err)
			ips, _ = middleware.NewIPResolver(nil)
		}
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recovery)
	r.Use(middleware.SecurityHeaders)
	r.Use(middleware.Logger(ips))
	r.Use(middleware.CORS(cfg.CORSAllowedOrigins))
	if deps.Limiter != nil {
		r.Use(deps.Limiter.Middleware)
	}
	r.Use(middleware.APIKeyAuth(cfg.APIKeys(), middleware.AuthOptions{
		MetricsPublic: cfg.MetricsPublic,
		Verifier:      keyVerifier(deps),
	}))

	probes := handlers.NewProbes(readiness(deps))
	r.Get("/healthz", probes.Live)
	r.Get("/readyz", probes.Ready)
	r.Get("/metrics", deps.Handler.Metrics)

	if cfg.DocsEnabled {
		r.Get("/docs", handlers.HandleSwaggerUI)
		r.Get("/docs/", handlers.HandleSwaggerUI)
		r.Get("/docs/openapi.json", handlers.HandleOpenAPISpec)
		r.Get("/swagger", routes.RedirectToDocs)
		r.Get("/swagger/", routes.RedirectToDocs)
	}

	r.Group(func(api chi.Router) {
		if cfg.RequestTimeout > 0 {
			api.Use(middleware.Timeout(cfg.RequestTimeout))
		}
		routes.RegisterRoutes(api, deps.Handler, cfg.APIBasePath)
		if deps.KeyAPI != nil {
			routes.RegisterAuthRoutes(api, deps.KeyAPI, cfg.APIBasePath, middleware.RequireAdmin)
		}
	})
	return r
}

// keyVerifier resolves managed (expiring) API keys to a non-admin principal.
func keyVerifier(deps *Dependencies) middleware.KeyVerifier {
	if deps.Keys == nil {
		return nil
	}
	return func(ctx context.Context, key string) (*middleware.Principal, error) {
		k, err := deps.Keys.Verify(ctx, key)
		if err != nil {
			return nil, err
		}
		return &middleware.Principal{KeyID: k.ID, Name: k.Name}, nil
	}
}

// readiness is ready when the job store answers and the service still accepts jobs.
func readiness(deps *Dependencies) handlers.ReadinessCheck {
	return func(ctx context.Context) error {
		if deps.Service != nil && !deps.Service.Jobs.Accepting() {
			return errors.New("shutting down")
		}
		if p, ok := deps.Store.(interface{ Ping(context.Context) error }); ok {
			if err := p.Ping(ctx); err != nil {
				return errors.New("job store unavailable")
			}
		}
		return nil
	}
}
