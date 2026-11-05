package routes

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/handlers"
)

// RegisterRoutes attaches pipeline routes to the provided chi router.
// An optional basePath parameter configures the API route prefix (default: "/api/v1").
//
// Probes and metrics are registered by the caller (see bootstrap.BuildRouter) so they can sit
// outside the API request timeout.
func RegisterRoutes(r chi.Router, ctrl *handlers.PipelineHandler, basePath ...string) {

	base := "/api/v1"
	if len(basePath) > 0 && strings.TrimSpace(basePath[0]) != "" {
		base = strings.TrimSpace(basePath[0])
	}
	base = "/" + strings.Trim(base, "/")
	pipelinePath := base + "/pipelines"
	if base == "/" {
		pipelinePath = "/pipelines"
	}

	// Pipeline API
	r.Route(pipelinePath, func(r chi.Router) {
		r.Post("/", ctrl.Create)
		r.Get("/", ctrl.List)

		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", ctrl.Get)
			r.Delete("/", ctrl.Delete)
			r.Get("/progress", ctrl.GetProgress)
			r.Get("/results", ctrl.GetResults)
			r.Get("/errors", ctrl.GetErrors)
			r.Patch("/cancel", ctrl.Cancel)
		})
	})
}

// RedirectToDocs redirects to documentation.
func RedirectToDocs(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/docs", http.StatusMovedPermanently)
}

// RegisterAuthRoutes mounts the admin-only API key management endpoints under
// <basePath>/auth/keys.
func RegisterAuthRoutes(r chi.Router, h *handlers.APIKeyHandler, basePath string, requireAdmin func(http.Handler) http.Handler) {
	if strings.TrimSpace(basePath) == "" {
		basePath = "/api/v1"
	}
	base := "/" + strings.Trim(strings.TrimSpace(basePath), "/")
	if base == "/" {
		base = ""
	}
	r.Route(base+"/auth/keys", func(r chi.Router) {
		r.Use(requireAdmin)
		r.Post("/", h.Create)
		r.Get("/", h.List)
		r.Delete("/{keyID}", h.Revoke)
	})
}
