package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/jobs"
	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(metrics.GlobalCollector.FormatPrometheus()))
}

func (s *Server) handlePipelinesCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.createPipelineJob(w, r)
	case http.MethodGet:
		s.listPipelineJobs(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) createPipelineJob(w http.ResponseWriter, r *http.Request) {
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
		if errors.Is(err, jobs.ErrCapacityReached) {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "submit job: "+err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (s *Server) listPipelineJobs(w http.ResponseWriter, r *http.Request) {
	jobList, err := s.manager.ListJobs(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list jobs: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, jobList)
}

func (s *Server) handlePipelineItem(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/pipelines/"), "/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "resource not found")
		return
	}

	id := parts[0]
	if len(parts) == 1 {
		s.handleJobDirect(w, r, id)
		return
	}
	if len(parts) == 2 {
		s.handleJobAction(w, r, id, parts[1])
		return
	}
	writeError(w, http.StatusNotFound, "resource not found")
}

func (s *Server) handleJobDirect(w http.ResponseWriter, r *http.Request, id string) {
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
			if errors.Is(err, jobs.ErrJobStillRunning) {
				writeError(w, http.StatusConflict, err.Error())
				return
			}
			if errors.Is(err, jobs.ErrJobNotFound) {
				writeError(w, http.StatusNotFound, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		w.Header().Set("Allow", "GET, DELETE")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleJobAction(w http.ResponseWriter, r *http.Request, id, action string) {
	switch action {
	case "progress":
		s.handleJobProgress(w, r, id)
	case "results":
		s.handleJobResults(w, r, id)
	case "errors":
		s.handleJobErrors(w, r, id)
	case "cancel":
		s.handleJobCancel(w, r, id)
	default:
		writeError(w, http.StatusNotFound, "resource not found")
	}
}

func (s *Server) handleJobProgress(w http.ResponseWriter, r *http.Request, id string) {
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
}

func (s *Server) handleJobResults(w http.ResponseWriter, r *http.Request, id string) {
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
}

func (s *Server) handleJobErrors(w http.ResponseWriter, r *http.Request, id string) {
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
	writeJSON(w, http.StatusOK, map[string]any{"job_id": id, "errors": errs})
}

func (s *Server) handleJobCancel(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPatch {
		w.Header().Set("Allow", "PATCH")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if err := s.manager.CancelJob(id); err != nil {
		if errors.Is(err, jobs.ErrJobNotRunning) {
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
}
