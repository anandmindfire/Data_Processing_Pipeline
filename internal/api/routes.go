package api

import (
	"net/http"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/middleware"
	"github.com/go-chi/chi/v5"
)

// setupRoutes initializes URL routing using the chi router and middleware decorators.
func (s *Server) setupRoutes() {
	r := chi.NewRouter()

	// Global middleware chain
	r.Use(middleware.Recovery)
	r.Use(middleware.Logger)
	if s.rateLimiter != nil {
		r.Use(s.rateLimiter.Middleware)
	}

	// Observability & Documentation
	r.Get("/healthz", s.handleHealth)
	r.Get("/metrics", s.handleMetrics)
	r.Get("/docs", s.handleSwaggerUI)
	r.Get("/docs/", s.handleSwaggerUI)
	r.Get("/docs/openapi.json", s.handleOpenAPISpec)
	r.Get("/swagger", redirectDocs)
	r.Get("/swagger/", redirectDocs)

	// Pipeline Job Management REST API
	r.Route("/api/v1/pipelines", func(r chi.Router) {
		r.Post("/", s.createPipelineJob)
		r.Get("/", s.listPipelineJobs)

		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", s.getPipelineJob)
			r.Delete("/", s.deletePipelineJob)
			r.Get("/progress", s.getPipelineProgress)
			r.Get("/results", s.getPipelineResults)
			r.Get("/errors", s.getPipelineErrors)
			r.Patch("/cancel", s.cancelPipelineJob)
		})
	})

	s.handler = r
}

func redirectDocs(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/docs", http.StatusMovedPermanently)
}
