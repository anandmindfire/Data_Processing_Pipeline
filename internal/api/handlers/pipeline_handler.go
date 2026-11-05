package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/dto/request"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/dto/response"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/respond"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/validator"
	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/services"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

// PipelineHandler handles HTTP requests for pipeline orchestration.
type PipelineHandler struct {
	service             *services.PipelineService
	maxRequestBodyBytes int64
	validator           *validator.RequestValidator
}

// Pagination bounds for list endpoints.
const (
	defaultJobsLimit    = 50
	defaultRecordsLimit = 100
	maxPageLimit        = 1000
	// HeaderTotalCount carries the total number of matching items for paginated responses.
	HeaderTotalCount = "X-Total-Count"
)

// NewPipelineHandler creates a new PipelineHandler instance.
func NewPipelineHandler(service *services.PipelineService, maxRequestBodyBytes int64, policy validator.Policy) *PipelineHandler {
	if maxRequestBodyBytes <= 0 {
		maxRequestBodyBytes = 2 << 20
	}
	return &PipelineHandler{
		service:             service,
		maxRequestBodyBytes: maxRequestBodyBytes,
		validator:           validator.NewRequestValidator(policy),
	}
}

// parsePage reads ?offset= and ?limit= (bounded by maxPageLimit).
func parsePage(r *http.Request, defaultLimit int) (offset, limit int, err error) {
	q := r.URL.Query()
	parse := func(name string, def int) (int, error) {
		raw := strings.TrimSpace(q.Get(name))
		if raw == "" {
			return def, nil
		}
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("%s must be a non-negative integer", name)
		}
		return n, nil
	}
	if offset, err = parse("offset", 0); err != nil {
		return 0, 0, err
	}
	if limit, err = parse("limit", defaultLimit); err != nil {
		return 0, 0, err
	}
	if limit == 0 || limit > maxPageLimit {
		return 0, 0, fmt.Errorf("limit must be between 1 and %d", maxPageLimit)
	}
	return offset, limit, nil
}

// Metrics exports Prometheus scrapable telemetry metrics.
func (c *PipelineHandler) Metrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set(respond.HeaderContentType, "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(metrics.GlobalCollector.FormatPrometheus()))
}

// Create handles pipeline creation submissions.
func (c *PipelineHandler) Create(w http.ResponseWriter, r *http.Request) {
	if ct := r.Header.Get(respond.HeaderContentType); ct != "" && !strings.HasPrefix(strings.ToLower(ct), "application/json") {
		respond.Error(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}
	var req request.CreatePipelineRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, c.maxRequestBodyBytes))
	dec.DisallowUnknownFields() // typos such as "validation_worker" must fail loudly, not be ignored
	if err := dec.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			respond.Error(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeds %d bytes", tooLarge.Limit))
			return
		}
		respond.Error(w, http.StatusBadRequest, "invalid request JSON: "+err.Error())
		return
	}
	if dec.More() {
		respond.Error(w, http.StatusBadRequest, "invalid request JSON: unexpected data after the job specification")
		return
	}

	cfg := req.ToSpec()
	if err := c.validator.Validate(&cfg); err != nil {
		respond.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	job, err := c.service.SubmitJob(r.Context(), cfg)
	if err != nil {
		if errors.Is(err, services.ErrCapacityReached) || errors.Is(err, services.ErrShuttingDown) {
			w.Header().Set("Retry-After", "5")
			respond.Error(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		respond.Error(w, http.StatusInternalServerError, "submit job: "+err.Error())
		return
	}
	respond.JSON(w, http.StatusAccepted, response.FromJob(job))
}

// List returns a page of pipeline jobs, newest first. Supports ?status=, ?limit=, ?offset=;
// the total match count is returned in the X-Total-Count header.
func (c *PipelineHandler) List(w http.ResponseWriter, r *http.Request) {
	offset, limit, err := parsePage(r, defaultJobsLimit)
	if err != nil {
		respond.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	status := models.JobStatus(strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("status"))))
	switch status {
	case "", models.StatusPending, models.StatusRunning, models.StatusCompleted, models.StatusFailed, models.StatusCancelled:
	default:
		respond.Error(w, http.StatusBadRequest, "status must be one of PENDING, RUNNING, COMPLETED, FAILED, CANCELLED")
		return
	}
	jobList, total, err := c.service.ListJobs(r.Context(), storage.ListOptions{Status: status, Offset: offset, Limit: limit})
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, "list jobs: "+err.Error())
		return
	}
	respList := make([]response.PipelineResponse, len(jobList))
	for i := range jobList {
		respList[i] = response.FromJob(&jobList[i])
	}
	w.Header().Set(HeaderTotalCount, strconv.Itoa(total))
	respond.JSON(w, http.StatusOK, respList)
}

// Get returns details for a specific pipeline job.
func (c *PipelineHandler) Get(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job, found, err := c.service.GetJob(r.Context(), id)
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		respond.Error(w, http.StatusNotFound, "pipeline job not found")
		return
	}
	respond.JSON(w, http.StatusOK, response.FromJob(job))
}

// Delete removes a finished pipeline job.
func (c *PipelineHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	err := c.service.DeleteJob(r.Context(), id)
	if err != nil {
		if errors.Is(err, services.ErrJobStillRunning) {
			respond.Error(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, services.ErrJobNotFound) {
			respond.Error(w, http.StatusNotFound, err.Error())
			return
		}
		respond.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetProgress retrieves progress for a running or finished job.
func (c *PipelineHandler) GetProgress(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	progress, found, err := c.service.GetProgress(r.Context(), id)
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		respond.Error(w, http.StatusNotFound, "pipeline job not found")
		return
	}
	respond.JSON(w, http.StatusOK, response.FromProgress(progress))
}

// GetResults returns aggregations, export paths, and a page of processed records (?limit=, ?offset=).
func (c *PipelineHandler) GetResults(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	offset, limit, err := parsePage(r, defaultRecordsLimit)
	if err != nil {
		respond.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	results, found, err := c.service.GetResults(r.Context(), id, offset, limit)
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		respond.Error(w, http.StatusNotFound, "pipeline job not found")
		return
	}
	respond.JSON(w, http.StatusOK, results)
}

// GetErrors returns a page of error events and failed records for a job (?limit=, ?offset=).
func (c *PipelineHandler) GetErrors(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	offset, limit, err := parsePage(r, defaultRecordsLimit)
	if err != nil {
		respond.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	errs, total, found, err := c.service.GetErrors(r.Context(), id, offset, limit)
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		respond.Error(w, http.StatusNotFound, "pipeline job not found")
		return
	}
	respond.JSON(w, http.StatusOK, response.ErrorsListResponse{
		JobID:  id,
		Total:  total,
		Offset: offset,
		Limit:  limit,
		Errors: errs,
	})
}

// Cancel interrupts an actively executing pipeline job.
func (c *PipelineHandler) Cancel(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := c.service.CancelJob(r.Context(), id); err != nil {
		switch {
		case errors.Is(err, services.ErrJobNotFound):
			respond.Error(w, http.StatusNotFound, "pipeline job not found")
		case errors.Is(err, services.ErrJobNotRunning):
			respond.Error(w, http.StatusConflict, err.Error())
		default:
			respond.Error(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	respond.JSON(w, http.StatusAccepted, map[string]string{
		"status": "cancellation_requested",
		"job_id": id,
	})
}
