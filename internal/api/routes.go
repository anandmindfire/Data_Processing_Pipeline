package api

import (
	"net/http"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/middleware"
)

// setupRoutes initializes URL routing with middleware decorators.
func (s *Server) setupRoutes() {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/docs", s.handleSwaggerUI)
	mux.HandleFunc("/docs/", s.handleSwaggerUI)
	mux.HandleFunc("/docs/openapi.json", s.handleOpenAPISpec)
	mux.HandleFunc("/swagger", redirectDocs)
	mux.HandleFunc("/swagger/", redirectDocs)
	mux.HandleFunc("/api/v1/pipelines", s.handlePipelinesCollection)
	mux.HandleFunc("/api/v1/pipelines/", s.handlePipelineItem)

	var handler http.Handler = mux
	if s.rateLimiter != nil {
		handler = s.rateLimiter.Middleware(handler)
	}
	handler = middleware.Logger(handler)
	handler = middleware.Recovery(handler)

	s.handler = handler
}

func redirectDocs(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/docs", http.StatusMovedPermanently)
}
