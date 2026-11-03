package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/anandkr47/data-processing-pipeline/internal/pipeline"
)

// handlePipelineItem routes item-level operations (/api/v1/pipelines/:id and sub-resources).
func (s *Server) handlePipelineItem(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/pipelines/"), "/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "resource not found")
		return
	}

	id := parts[0]

	// Route: /api/v1/pipelines/:id
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			job, found, err := s.manager.GetJob(r.Context(), id)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			if !found {
				writeError(w, http.StatusNotFound, "pipeline job not found")
				return
			}
			writeJSON(w, http.StatusOK, job)

		case http.MethodDelete:
			err := s.manager.DeleteJob(r.Context(), id)
			if err != nil {
				if errors.Is(err, pipeline.ErrJobStillRunning) {
					writeError(w, http.StatusConflict, err.Error())
					return
				}
				if errors.Is(err, pipeline.ErrJobNotFound) {
					writeError(w, http.StatusNotFound, err.Error())
					return
				}
				writeError(w, http.StatusInternalServerError, "failed to delete job: "+err.Error())
				return
			}
			w.WriteHeader(http.StatusNoContent)

		default:
			w.Header().Set("Allow", "GET, DELETE")
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}

	// Route: /api/v1/pipelines/:id/<action>
	if len(parts) != 2 {
		writeError(w, http.StatusNotFound, "resource not found")
		return
	}

	action := parts[1]
	switch action {
	case "progress":
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		progress, found, err := s.manager.GetProgress(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "pipeline job not found")
			return
		}
		writeJSON(w, http.StatusOK, progress)

	case "results":
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		results, found, err := s.manager.GetResults(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "pipeline job not found")
			return
		}
		writeJSON(w, http.StatusOK, results)

	case "errors":
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		errs, found, err := s.manager.GetErrors(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "pipeline job not found")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"job_id": id,
			"errors": errs,
		})

	case "cancel":
		if r.Method != http.MethodPatch {
			w.Header().Set("Allow", "PATCH")
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		err := s.manager.CancelJob(id)
		if err != nil {
			if errors.Is(err, pipeline.ErrJobNotRunning) {
				writeError(w, http.StatusConflict, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{
			"status": "cancellation_requested",
			"job_id": id,
		})

	default:
		writeError(w, http.StatusNotFound, "resource not found")
	}
}

