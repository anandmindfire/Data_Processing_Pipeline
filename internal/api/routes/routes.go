package routes

import (
	"net/http"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/controllers"
	"github.com/go-chi/chi/v5"
)

// RegisterRoutes attaches pipeline routes to the provided chi router.
// An optional basePath parameter configures the API route prefix (default: "/api/v1").
func RegisterRoutes(r chi.Router, ctrl *controllers.PipelineController, basePath ...string) {
	// Observability
	r.Get("/healthz", ctrl.Health)
	r.Get("/metrics", ctrl.Metrics)

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
