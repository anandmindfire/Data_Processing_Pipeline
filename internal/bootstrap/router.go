package bootstrap

import (
	"net/http"

	"github.com/anandmindfire/data-processing-pipeline/internal/api"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/middleware"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/routes"
	"github.com/go-chi/chi/v5"
)

// BuildRouter constructs the chi HTTP router with the standard middleware pipeline and route hierarchy.
func BuildRouter(deps *Dependencies) http.Handler {
	r := chi.NewRouter()

	// Global middleware stack
	r.Use(middleware.Recovery)
	r.Use(middleware.SecurityHeaders)
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)
	r.Use(middleware.CORS)

	if deps.Config.APIKey != "" {
		r.Use(middleware.APIKeyAuth(deps.Config.APIKey))
	}

	if deps.Config.RateLimitRPS > 0 && deps.Config.RateLimitBurst > 0 {
		limiter := middleware.NewRateLimiter(deps.Config.RateLimitRPS, deps.Config.RateLimitBurst)
		r.Use(limiter.Middleware)
	}

	// Documentation & Swagger endpoints
	r.Get("/docs", api.HandleSwaggerUI)
	r.Get("/docs/", api.HandleSwaggerUI)
	r.Get("/docs/openapi.json", api.HandleOpenAPISpec)
	r.Get("/swagger", routes.RedirectToDocs)
	r.Get("/swagger/", routes.RedirectToDocs)

	// API Routes
	routes.RegisterRoutes(r, deps.Controller)

	return r
}
