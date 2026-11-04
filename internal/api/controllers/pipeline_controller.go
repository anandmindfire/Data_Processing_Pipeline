package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/dto/request"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/dto/response"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/validator"
	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/services"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
	"github.com/anandmindfire/data-processing-pipeline/pkg/constants"
	pkgerrors "github.com/anandmindfire/data-processing-pipeline/pkg/errors"
	pkgresponse "github.com/anandmindfire/data-processing-pipeline/pkg/response"
	"github.com/go-chi/chi/v5"
)

// PipelineController handles HTTP requests for pipeline orchestration.
type PipelineController struct {
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

// NewPipelineController creates a new PipelineController instance.
func NewPipelineController(service *services.PipelineService, maxRequestBodyBytes int64, policy validator.Policy) *PipelineController {
	if maxRequestBodyBytes <= 0 {
		maxRequestBodyBytes = 2 << 20
	}
	return &PipelineController{
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

// Health responds with a simple system health status.
func (c *PipelineController) Health(w http.ResponseWriter, _ *http.Request) {
	pkgresponse.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Metrics exports Prometheus scrapable telemetry metrics.
func (c *PipelineController) Metrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set(constants.HeaderContentType, "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(metrics.GlobalCollector.FormatPrometheus()))
}

// Create handles pipeline creation submissions.
func (c *PipelineController) Create(w http.ResponseWriter, r *http.Request) {
	var req request.CreatePipelineRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, c.maxRequestBodyBytes))
	if err := dec.Decode(&req); err != nil {
		pkgresponse.Error(w, http.StatusBadRequest, "invalid request JSON: "+err.Error())
		return
	}

	cfg := req.ToConfig()
	if err := c.validator.Validate(&cfg); err != nil {
		pkgresponse.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	job, err := c.service.SubmitJob(r.Context(), cfg)
	if err != nil {
		if errors.Is(err, pkgerrors.ErrCapacityReached) || errors.Is(err, pkgerrors.ErrShuttingDown) {
			w.Header().Set("Retry-After", "5")
			pkgresponse.Error(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		pkgresponse.Error(w, http.StatusInternalServerError, "submit job: "+err.Error())
		return
	}
	pkgresponse.JSON(w, http.StatusAccepted, response.FromJob(job))
}

// List returns a page of pipeline jobs, newest first. Supports ?status=, ?limit=, ?offset=;
// the total match count is returned in the X-Total-Count header.
func (c *PipelineController) List(w http.ResponseWriter, r *http.Request) {
	offset, limit, err := parsePage(r, defaultJobsLimit)
	if err != nil {
		pkgresponse.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	status := models.JobStatus(strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("status"))))
	switch status {
	case "", models.StatusPending, models.StatusRunning, models.StatusCompleted, models.StatusFailed, models.StatusCancelled:
	default:
		pkgresponse.Error(w, http.StatusBadRequest, "status must be one of PENDING, RUNNING, COMPLETED, FAILED, CANCELLED")
		return
	}
	jobList, total, err := c.service.ListJobs(r.Context(), storage.ListOptions{Status: status, Offset: offset, Limit: limit})
	if err != nil {
		pkgresponse.Error(w, http.StatusInternalServerError, "list jobs: "+err.Error())
		return
	}
	respList := make([]response.PipelineResponse, len(jobList))
	for i := range jobList {
		respList[i] = response.FromJob(&jobList[i])
	}
	w.Header().Set(HeaderTotalCount, strconv.Itoa(total))
	pkgresponse.JSON(w, http.StatusOK, respList)
}

// Get returns details for a specific pipeline job.
func (c *PipelineController) Get(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job, found, err := c.service.GetJob(r.Context(), id)
	if err != nil {
		pkgresponse.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		pkgresponse.Error(w, http.StatusNotFound, "pipeline job not found")
		return
	}
	pkgresponse.JSON(w, http.StatusOK, response.FromJob(job))
}

// Delete removes a finished pipeline job.
func (c *PipelineController) Delete(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	err := c.service.DeleteJob(r.Context(), id)
	if err != nil {
		if errors.Is(err, pkgerrors.ErrJobStillRunning) {
			pkgresponse.Error(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, pkgerrors.ErrJobNotFound) {
			pkgresponse.Error(w, http.StatusNotFound, err.Error())
			return
		}
		pkgresponse.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetProgress retrieves progress for a running or finished job.
func (c *PipelineController) GetProgress(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	progress, found, err := c.service.GetProgress(r.Context(), id)
	if err != nil {
		pkgresponse.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		pkgresponse.Error(w, http.StatusNotFound, "pipeline job not found")
		return
	}
	pkgresponse.JSON(w, http.StatusOK, response.FromProgress(progress))
}

// GetResults returns aggregations, export paths, and a page of processed records (?limit=, ?offset=).
func (c *PipelineController) GetResults(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	offset, limit, err := parsePage(r, defaultRecordsLimit)
	if err != nil {
		pkgresponse.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	results, found, err := c.service.GetResults(r.Context(), id, offset, limit)
	if err != nil {
		pkgresponse.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		pkgresponse.Error(w, http.StatusNotFound, "pipeline job not found")
		return
	}
	pkgresponse.JSON(w, http.StatusOK, results)
}

// GetErrors returns a page of error events and failed records for a job (?limit=, ?offset=).
func (c *PipelineController) GetErrors(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	offset, limit, err := parsePage(r, defaultRecordsLimit)
	if err != nil {
		pkgresponse.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	errs, total, found, err := c.service.GetErrors(r.Context(), id, offset, limit)
	if err != nil {
		pkgresponse.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		pkgresponse.Error(w, http.StatusNotFound, "pipeline job not found")
		return
	}
	pkgresponse.JSON(w, http.StatusOK, response.ErrorsListResponse{
		JobID:  id,
		Total:  total,
		Offset: offset,
		Limit:  limit,
		Errors: errs,
	})
}

// Cancel interrupts an actively executing pipeline job.
func (c *PipelineController) Cancel(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := c.service.CancelJob(id); err != nil {
		if errors.Is(err, pkgerrors.ErrJobNotRunning) {
			if _, found, _ := c.service.GetJob(r.Context(), id); !found {
				pkgresponse.Error(w, http.StatusNotFound, "pipeline job not found")
				return
			}
			pkgresponse.Error(w, http.StatusConflict, err.Error())
			return
		}
		pkgresponse.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	pkgresponse.JSON(w, http.StatusAccepted, map[string]string{
		"status": "cancellation_requested",
		"job_id": id,
	})
}
