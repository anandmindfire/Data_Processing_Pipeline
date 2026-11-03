package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
	"github.com/anandkr47/data-processing-pipeline/internal/pipeline"
)

// handlePipelinesCollection manages collection-level operations: POST (create) and GET (list).
func (s *Server) handlePipelinesCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var cfg models.Config
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, s.config.MaxRequestBodyBytes))
		if err := dec.Decode(&cfg); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request JSON: "+err.Error())
			return
		}

		if err := ValidateJobConfig(&cfg); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		job, err := s.manager.SubmitJob(r.Context(), cfg)
		if err != nil {
			if errors.Is(err, pipeline.ErrCapacityReached) {
				writeError(w, http.StatusServiceUnavailable, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, "failed to submit job: "+err.Error())
			return
		}

		writeJSON(w, http.StatusAccepted, job)

	case http.MethodGet:
		jobs, err := s.manager.ListJobs(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list jobs: "+err.Error())
			return
		}
		writeJSON(w, http.StatusOK, jobs)

	default:
		w.Header().Set("Allow", "GET, POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

