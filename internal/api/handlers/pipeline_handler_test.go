package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/dto/request"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/handlers"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/validator"
	"github.com/anandmindfire/data-processing-pipeline/internal/config"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/services"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

type mockRunner struct {
	execFn func(ctx context.Context, job *models.Job, onUpdate func(*models.Job))
}

func (m *mockRunner) Execute(ctx context.Context, job *models.Job, onUpdate func(*models.Job)) {
	if m.execFn != nil {
		m.execFn(ctx, job, onUpdate)
	}
}

func setupTestHandler(t *testing.T) (*handlers.PipelineHandler, *services.PipelineService, storage.Repository, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	cfg.ExportDir = filepath.Join(dir, "exports")
	cfg.InputDirs = []string{dir}
	_ = os.MkdirAll(cfg.ExportDir, 0o755)

	store := storage.NewMemoryRepository()
	runner := &mockRunner{}
	service := services.NewPipelineService(cfg, store, runner)
	policy := validator.Policy{
		InputDirs:  []string{dir},
		ExportDir:  cfg.ExportDir,
		MaxWorkers: 10,
		MaxBuffer:  100,
		MaxBatch:   100,
		MaxTimeout: 600,
		MaxSources: 5,
		MaxExports: 5,
	}
	h := handlers.NewPipelineHandler(service, 1<<20, policy)
	return h, service, store, dir
}

func withChiURLParam(req *http.Request, key, val string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add(key, val)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

func TestHealthAndMetrics(t *testing.T) {
	h, _, _, _ := setupTestHandler(t)

	// Health
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handlers.NewProbes(nil).Live(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Fatalf("Health check failed: code %d, body %s", rec.Code, rec.Body.String())
	}

	// Metrics
	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec = httptest.NewRecorder()
	h.Metrics(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Metrics failed with code %d", rec.Code)
	}
}

func TestCreateJobEndpoints(t *testing.T) {
	h, _, _, dir := setupTestHandler(t)
	inputCSV := filepath.Join(dir, "input.csv")
	_ = os.WriteFile(inputCSV, []byte("name,age\nAlice,30\n"), 0o600)

	t.Run("invalid json body", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", strings.NewReader("invalid-json"))
		rec := httptest.NewRecorder()
		h.Create(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for bad json, got %d", rec.Code)
		}
	})

	t.Run("validation failure", func(t *testing.T) {
		body, _ := json.Marshal(request.CreatePipelineRequest{})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		h.Create(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for empty spec, got %d", rec.Code)
		}
	})

	t.Run("successful submission", func(t *testing.T) {
		specReq := request.CreatePipelineRequest{
			Sources: []models.Source{{Type: "csv", Name: "test", Path: inputCSV}},
			Exports: []models.ExportTarget{{Type: "json", Path: filepath.Join(dir, "exports", "out.json")}},
		}
		body, _ := json.Marshal(specReq)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		h.Create(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("expected 202 Accepted, got %d (body: %s)", rec.Code, rec.Body.String())
		}
	})
}

func TestListAndGetJobs(t *testing.T) {
	h, _, store, _ := setupTestHandler(t)
	ctx := context.Background()

	job1 := &models.Job{ID: "job-1", Status: models.StatusCompleted, CreatedAt: time.Now().Add(-time.Hour), UpdatedAt: time.Now()}
	job2 := &models.Job{ID: "job-2", Status: models.StatusRunning, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	_ = store.SaveJob(ctx, job1)
	_ = store.SaveJob(ctx, job2)

	t.Run("list all jobs", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
		rec := httptest.NewRecorder()
		h.List(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		if total := rec.Header().Get(handlers.HeaderTotalCount); total != "2" {
			t.Errorf("expected X-Total-Count 2, got %s", total)
		}
	})

	t.Run("list with invalid pagination or status", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines?limit=-5", nil)
		rec := httptest.NewRecorder()
		h.List(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for negative limit, got %d", rec.Code)
		}

		req = httptest.NewRequest(http.MethodGet, "/api/v1/pipelines?status=INVALID_STATUS", nil)
		rec = httptest.NewRecorder()
		h.List(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for invalid status filter, got %d", rec.Code)
		}
	})

	t.Run("get existing and missing job", func(t *testing.T) {
		req := withChiURLParam(httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/job-1", nil), "id", "job-1")
		rec := httptest.NewRecorder()
		h.Get(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("expected 200 for job-1, got %d", rec.Code)
		}

		reqMissing := withChiURLParam(httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/missing", nil), "id", "missing")
		recMissing := httptest.NewRecorder()
		h.Get(recMissing, reqMissing)
		if recMissing.Code != http.StatusNotFound {
			t.Errorf("expected 404 for missing job, got %d", recMissing.Code)
		}
	})
}

func TestDeleteAndCancelJob(t *testing.T) {
	h, service, store, _ := setupTestHandler(t)
	ctx := context.Background()

	job := &models.Job{ID: "job-del", Status: models.StatusCompleted, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	_ = store.SaveJob(ctx, job)

	t.Run("delete existing completed job", func(t *testing.T) {
		req := withChiURLParam(httptest.NewRequest(http.MethodDelete, "/api/v1/pipelines/job-del", nil), "id", "job-del")
		rec := httptest.NewRecorder()
		h.Delete(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Errorf("expected 204 for deleted job, got %d", rec.Code)
		}
	})

	t.Run("delete missing job", func(t *testing.T) {
		req := withChiURLParam(httptest.NewRequest(http.MethodDelete, "/api/v1/pipelines/missing", nil), "id", "missing")
		rec := httptest.NewRecorder()
		h.Delete(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("expected 404 for missing job, got %d", rec.Code)
		}
	})

	t.Run("delete running job returns conflict", func(t *testing.T) {
		// Mock running job by submitting to service with blocking runner
		blockChan := make(chan struct{})
		defer close(blockChan)
		runningRunner := &mockRunner{
			execFn: func(ctx context.Context, job *models.Job, _ func(*models.Job)) {
				<-blockChan
			},
		}
		runService := services.NewPipelineService(config.DefaultConfig(), store, runningRunner)
		runCtrl := handlers.NewPipelineHandler(runService, 1<<20, validator.Policy{})
		submittedJob, err := runService.SubmitJob(ctx, models.JobSpec{
			Sources: []models.Source{{Type: "csv", Name: "s"}},
			Exports: []models.ExportTarget{{Type: "json"}},
		})
		if err == nil {
			req := withChiURLParam(httptest.NewRequest(http.MethodDelete, "/api/v1/pipelines/"+submittedJob.ID, nil), "id", submittedJob.ID)
			rec := httptest.NewRecorder()
			runCtrl.Delete(rec, req)
			if rec.Code != http.StatusConflict {
				t.Errorf("expected 409 for deleting running job, got %d", rec.Code)
			}

			// Also test cancel running job (202 Accepted)
			cancelReq := withChiURLParam(httptest.NewRequest(http.MethodPatch, "/api/v1/pipelines/"+submittedJob.ID+"/cancel", nil), "id", submittedJob.ID)
			cancelRec := httptest.NewRecorder()
			runCtrl.Cancel(cancelRec, cancelReq)
			if cancelRec.Code != http.StatusAccepted {
				t.Errorf("expected 202 for cancelling running job, got %d", cancelRec.Code)
			}
		}
	})

	t.Run("cancel missing job", func(t *testing.T) {
		req := withChiURLParam(httptest.NewRequest(http.MethodPatch, "/api/v1/pipelines/missing/cancel", nil), "id", "missing")
		rec := httptest.NewRecorder()
		h.Cancel(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("expected 404 for cancelling missing job, got %d", rec.Code)
		}
	})

	t.Run("cancel completed job returns conflict", func(t *testing.T) {
		completedJob := &models.Job{ID: "job-comp", Status: models.StatusCompleted, CreatedAt: time.Now(), UpdatedAt: time.Now()}
		_ = store.SaveJob(ctx, completedJob)
		req := withChiURLParam(httptest.NewRequest(http.MethodPatch, "/api/v1/pipelines/job-comp/cancel", nil), "id", "job-comp")
		rec := httptest.NewRecorder()
		h.Cancel(rec, req)
		if rec.Code != http.StatusConflict {
			t.Errorf("expected 409 for cancelling non-running job, got %d", rec.Code)
		}
		_ = service
	})
}

func TestGetProgressResultsAndErrors(t *testing.T) {
	h, _, store, _ := setupTestHandler(t)
	ctx := context.Background()

	jobID := "job-inspect"
	job := &models.Job{
		ID:        jobID,
		Status:    models.StatusCompleted,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Metrics: models.Metrics{
			Processed:  10,
			Discovered: 10,
			Valid:      10,
		},
		Aggregates: map[string]any{"count": 1.0},
	}
	_ = store.SaveJob(ctx, job)
	_ = store.SaveRecords(ctx, jobID, []models.Record{{"id": 1.0, "name": "Item"}})
	_ = store.AddErrors(ctx, jobID, []models.RecordError{{Stage: "validation", Message: "invalid field", At: time.Now()}})

	t.Run("get progress", func(t *testing.T) {
		req := withChiURLParam(httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID+"/progress", nil), "id", jobID)
		rec := httptest.NewRecorder()
		h.GetProgress(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
	})

	t.Run("get results", func(t *testing.T) {
		req := withChiURLParam(httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID+"/results", nil), "id", jobID)
		rec := httptest.NewRecorder()
		h.GetResults(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}

		reqBad := withChiURLParam(httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID+"/results?limit=-1", nil), "id", jobID)
		recBad := httptest.NewRecorder()
		h.GetResults(recBad, reqBad)
		if recBad.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for negative limit, got %d", recBad.Code)
		}
	})

	t.Run("get errors", func(t *testing.T) {
		req := withChiURLParam(httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID+"/errors", nil), "id", jobID)
		rec := httptest.NewRecorder()
		h.GetErrors(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}

		reqBad := withChiURLParam(httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID+"/errors?limit=-1", nil), "id", jobID)
		recBad := httptest.NewRecorder()
		h.GetErrors(recBad, reqBad)
		if recBad.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for negative limit, got %d", recBad.Code)
		}
	})

	t.Run("missing job inspection returns 404", func(t *testing.T) {
		for _, endpoint := range []string{"progress", "results", "errors"} {
			req := withChiURLParam(httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/nonexistent/"+endpoint, nil), "id", "nonexistent")
			rec := httptest.NewRecorder()
			switch endpoint {
			case "progress":
				h.GetProgress(rec, req)
			case "results":
				h.GetResults(rec, req)
			case "errors":
				h.GetErrors(rec, req)
			}
			if rec.Code != http.StatusNotFound {
				t.Errorf("endpoint %s expected 404, got %d", endpoint, rec.Code)
			}
		}
	})
}

func TestReadinessProbe(t *testing.T) {
	rec := httptest.NewRecorder()
	handlers.NewProbes(func(context.Context) error { return errors.New("db down") }).Ready(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when not ready, got %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	handlers.NewProbes(func(context.Context) error { return nil }).Ready(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 when ready, got %d", rec.Code)
	}
}
