package api

import (
	"net/http"

	"github.com/anandkr47/data-processing-pipeline/internal/api/middleware"
)

// setupRoutes initializes URL routing with middleware decorators.
func (s *Server) setupRoutes() {
	mux := http.NewServeMux()

	// Health and Observability
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/metrics", s.handleMetrics)

	// Job Management API v1
	mux.HandleFunc("/api/v1/pipelines", s.handlePipelinesCollection)
	mux.HandleFunc("/api/v1/pipelines/", s.handlePipelineItem)

	// Build middleware chain
	var handler http.Handler = mux
	if s.rateLimiter != nil {
		handler = s.rateLimiter.Middleware(handler)
	}
	handler = middleware.Logger(handler)
	handler = middleware.Recovery(handler)

	s.handler = handler
}

