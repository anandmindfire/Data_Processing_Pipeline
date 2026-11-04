package routes

import (
	"net/http"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/controllers"
	"github.com/go-chi/chi/v5"
)

// RegisterRoutes attaches pipeline routes to the provided chi router.
func RegisterRoutes(r chi.Router, ctrl *controllers.PipelineController) {
	// Observability
	r.Get("/healthz", ctrl.Health)
	r.Get("/metrics", ctrl.Metrics)

	// Pipeline API v1
	r.Route("/api/v1/pipelines", func(r chi.Router) {
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
