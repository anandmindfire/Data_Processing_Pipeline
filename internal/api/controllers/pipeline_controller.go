package controllers

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/dto/request"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/dto/response"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/validator"
	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/services"
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

// NewPipelineController creates a new PipelineController instance.
func NewPipelineController(service *services.PipelineService, maxRequestBodyBytes int64) *PipelineController {
	if maxRequestBodyBytes <= 0 {
		maxRequestBodyBytes = 2 << 20
	}
	return &PipelineController{
		service:             service,
		maxRequestBodyBytes: maxRequestBodyBytes,
		validator:           validator.NewRequestValidator(),
	}
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
		if errors.Is(err, pkgerrors.ErrCapacityReached) {
			pkgresponse.Error(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		pkgresponse.Error(w, http.StatusInternalServerError, "submit job: "+err.Error())
		return
	}
	pkgresponse.JSON(w, http.StatusAccepted, response.FromJob(job))
}

// List returns all registered pipeline jobs.
func (c *PipelineController) List(w http.ResponseWriter, r *http.Request) {
	jobList, err := c.service.ListJobs(r.Context())
	if err != nil {
		pkgresponse.Error(w, http.StatusInternalServerError, "list jobs: "+err.Error())
		return
	}
	respList := make([]response.PipelineResponse, len(jobList))
	for i := range jobList {
		respList[i] = response.FromJob(&jobList[i])
	}
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

// GetResults retrieves execution output records for a job.
func (c *PipelineController) GetResults(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	results, found, err := c.service.GetResults(r.Context(), id)
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

// GetErrors retrieves error events recorded for a job.
func (c *PipelineController) GetErrors(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	errs, found, err := c.service.GetErrors(r.Context(), id)
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
		Total:  len(errs),
		Errors: errs,
	})
}

// Cancel interrupts an actively executing pipeline job.
func (c *PipelineController) Cancel(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := c.service.CancelJob(id); err != nil {
		if errors.Is(err, pkgerrors.ErrJobNotRunning) {
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
