package api

import (
	"net/http"

	"github.com/anandkr47/data-processing-pipeline/internal/metrics"
)

// handleMetrics serves Prometheus-compatible text metrics.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	output := metrics.GlobalCollector.FormatPrometheus()
	_, _ = w.Write([]byte(output))
}

