package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/configs"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/controllers"
	"github.com/anandmindfire/data-processing-pipeline/internal/export"
	"github.com/anandmindfire/data-processing-pipeline/internal/ingestion"
	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/pipeline"
	"github.com/anandmindfire/data-processing-pipeline/internal/services"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
	"github.com/anandmindfire/data-processing-pipeline/internal/transformation"
	"github.com/anandmindfire/data-processing-pipeline/internal/validation"
)

func setupTestServer(t *testing.T) (*Server, *storage.Store, string) {
	dir := t.TempDir()
	store, err := storage.New(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	srv := New(store)
	return srv, store, dir
}

func TestHealthzAndMetrics(t *testing.T) {
	srv, _, _ := setupTestServer(t)

	// Test healthz
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz code %d", rec.Code)
	}

	// Test metrics
	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics code %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "pipeline_jobs_total") {
		t.Fatalf("expected prometheus metrics format, got: %s", body)
	}
}

func TestSwaggerDocsEndpoints(t *testing.T) {
	srv, _, _ := setupTestServer(t)

	// 1. Test GET /docs
	req := httptest.NewRequest(http.MethodGet, "/docs", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected /docs code 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("expected text/html, got: %s", rec.Header().Get("Content-Type"))
	}
	if !strings.Contains(rec.Body.String(), "SwaggerUIBundle") {
		t.Fatalf("expected SwaggerUIBundle in body")
	}

	// 2. Test GET /docs/openapi.json
	req = httptest.NewRequest(http.MethodGet, "/docs/openapi.json", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected /docs/openapi.json code 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("expected application/json, got: %s", rec.Header().Get("Content-Type"))
	}
	var spec map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &spec); err != nil {
		t.Fatalf("invalid OpenAPI JSON spec: %v", err)
	}
	if spec["openapi"] != "3.0.3" {
		t.Fatalf("expected openapi version 3.0.3, got: %v", spec["openapi"])
	}

	// 3. Test GET /swagger redirect
	req = httptest.NewRequest(http.MethodGet, "/swagger/", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusMovedPermanently {
		t.Fatalf("expected redirect 301, got %d", rec.Code)
	}
	if rec.Header().Get("Location") != "/docs" {
		t.Fatalf("expected redirect to /docs, got %s", rec.Header().Get("Location"))
	}
}

func TestPipelineLifecycleEndpoints(t *testing.T) {
	srv, store, dir := setupTestServer(t)

	inputPath := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(inputPath, []byte("name,salary\nAlice,100\nBob,bad\n"), 0644); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(dir, "out.json")

	cfg := models.Config{
		Sources: []models.Source{
			{Type: "csv", Name: "sample", Path: inputPath},
		},
		Rules: []models.Rule{
			{Field: "name", Type: "string", Required: true},
			{Field: "salary", Type: "number", Required: true},
		},
		Aggregations: []models.Aggregation{
			{Field: "salary", Operation: "sum", Name: "total_salary"},
		},
		Exports: []models.ExportTarget{
			{Type: "json", Path: outputPath},
		},
	}

	body, _ := json.Marshal(cfg)

	jobID := testCreateAndListPipeline(t, srv, body)
	waitForJobCompletion(t, store, jobID)
	testJobDetailsAndProgress(t, srv, jobID)
	testJobResultsAndErrors(t, srv, jobID)
	testJobCancelAndDeletion(t, srv, jobID)
}

func testCreateAndListPipeline(t *testing.T, srv http.Handler, body []byte) string {
	t.Helper()
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(body))
	createRec := httptest.NewRecorder()
	srv.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusAccepted {
		t.Fatalf("create status %d: %s", createRec.Code, createRec.Body.String())
	}

	var createdJob models.Job
	if err := json.Unmarshal(createRec.Body.Bytes(), &createdJob); err != nil {
		t.Fatal(err)
	}
	if createdJob.ID == "" {
		t.Fatal("expected non-empty job ID")
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
	listRec := httptest.NewRecorder()
	srv.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status %d", listRec.Code)
	}

	var jobs []models.Job
	if err := json.Unmarshal(listRec.Body.Bytes(), &jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != createdJob.ID {
		t.Fatalf("expected 1 job with id %s, got %d jobs", createdJob.ID, len(jobs))
	}

	return createdJob.ID
}

func waitForJobCompletion(t *testing.T, store *storage.Store, jobID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		j, ok := store.GetJob(jobID)
		if ok && (j.Status == models.StatusCompleted || j.Status == models.StatusFailed) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job %s did not complete within deadline", jobID)
}

func testJobDetailsAndProgress(t *testing.T, srv http.Handler, jobID string) {
	t.Helper()
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID, nil)
	getRec := httptest.NewRecorder()
	srv.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status %d: %s", getRec.Code, getRec.Body.String())
	}

	var fetchedJob models.Job
	if err := json.Unmarshal(getRec.Body.Bytes(), &fetchedJob); err != nil {
		t.Fatal(err)
	}
	if fetchedJob.Status != models.StatusCompleted {
		t.Fatalf("expected job status COMPLETED, got %s", fetchedJob.Status)
	}

	progReq := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID+"/progress", nil)
	progRec := httptest.NewRecorder()
	srv.ServeHTTP(progRec, progReq)
	if progRec.Code != http.StatusOK {
		t.Fatalf("progress status %d: %s", progRec.Code, progRec.Body.String())
	}

	var prog models.Progress
	if err := json.Unmarshal(progRec.Body.Bytes(), &prog); err != nil {
		t.Fatal(err)
	}
	if prog.Percent != 100 || prog.Processed != 1 || prog.Rejected != 1 || prog.ErrorCount != 1 {
		t.Fatalf("unexpected progress counts: %+v", prog)
	}
}

func testJobResultsAndErrors(t *testing.T, srv http.Handler, jobID string) {
	t.Helper()
	resReq := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID+"/results", nil)
	resRec := httptest.NewRecorder()
	srv.ServeHTTP(resRec, resReq)
	if resRec.Code != http.StatusOK {
		t.Fatalf("results status %d: %s", resRec.Code, resRec.Body.String())
	}

	var resultsPayload map[string]any
	if err := json.Unmarshal(resRec.Body.Bytes(), &resultsPayload); err != nil {
		t.Fatal(err)
	}
	recordsList, ok := resultsPayload["records"].([]any)
	if !ok || len(recordsList) != 1 {
		t.Fatalf("expected 1 record in results, got %v", resultsPayload["records"])
	}

	errReq := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID+"/errors", nil)
	errRec := httptest.NewRecorder()
	srv.ServeHTTP(errRec, errReq)
	if errRec.Code != http.StatusOK {
		t.Fatalf("errors status %d: %s", errRec.Code, errRec.Body.String())
	}

	var errPayload map[string]any
	if err := json.Unmarshal(errRec.Body.Bytes(), &errPayload); err != nil {
		t.Fatal(err)
	}
	errList, ok := errPayload["errors"].([]any)
	if !ok || len(errList) != 1 {
		t.Fatalf("expected 1 error in payload, got %v", errPayload["errors"])
	}
}

func testJobCancelAndDeletion(t *testing.T, srv http.Handler, jobID string) {
	t.Helper()
	cancelReq := httptest.NewRequest(http.MethodPatch, "/api/v1/pipelines/"+jobID+"/cancel", nil)
	cancelRec := httptest.NewRecorder()
	srv.ServeHTTP(cancelRec, cancelReq)
	if cancelRec.Code != http.StatusConflict {
		t.Fatalf("expected 409 for cancelling non-running job, got %d", cancelRec.Code)
	}

	delReq := httptest.NewRequest(http.MethodDelete, "/api/v1/pipelines/"+jobID, nil)
	delRec := httptest.NewRecorder()
	srv.ServeHTTP(delRec, delReq)
	if delRec.Code != http.StatusNoContent {
		t.Fatalf("delete status %d: %s", delRec.Code, delRec.Body.String())
	}

	getAfterDel := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID, nil)
	getAfterDelRec := httptest.NewRecorder()
	srv.ServeHTTP(getAfterDelRec, getAfterDel)
	if getAfterDelRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after deletion, got %d", getAfterDelRec.Code)
	}
}

func TestAPIValidationErrors(t *testing.T) {
	srv, _, _ := setupTestServer(t)

	// Bad JSON
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", strings.NewReader("not-json"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad JSON, got %d", rec.Code)
	}

	// Empty sources
	cfgEmptySources := models.Config{Exports: []models.ExportTarget{{Type: "json"}}}
	b, _ := json.Marshal(cfgEmptySources)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(b))
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty sources, got %d", rec.Code)
	}

	// Empty exports
	cfgEmptyExports := models.Config{Sources: []models.Source{{Type: "csv", Path: "in.csv"}}}
	b, _ = json.Marshal(cfgEmptyExports)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(b))
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty exports, got %d", rec.Code)
	}

	// Non-existent job
	req = httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/nonexistent-id", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}

	// Method not allowed
	req = httptest.NewRequest(http.MethodPut, "/api/v1/pipelines", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestCustomAPIBasePathAndOpenAPIDynamicURL(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.New(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	cfg := configs.DefaultConfig()
	cfg.APIBasePath = "/api/v2"
	cfg.APIBaseURL = "https://production.pipeline.io"
	cfg.ExportDir = filepath.Join(dir, "exports")

	repo := store.Repository()
	pipe := pipeline.NewPipeline(
		ingestion.NewReader(http.DefaultClient),
		validation.NewValidator(),
		transformation.NewTransformer(),
		export.NewExporter(cfg.ExportDir),
		repo,
		metrics.GlobalCollector,
	)
	service := services.NewPipelineService(cfg, repo, pipe)
	ctrl := controllers.NewPipelineController(service, cfg.MaxRequestBodyBytes)
	srv := NewServer(cfg, ctrl)

	// Custom route /api/v2/pipelines should match and return 200 OK
	req := httptest.NewRequest(http.MethodGet, "/api/v2/pipelines", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected /api/v2/pipelines to return 200, got %d", rec.Code)
	}

	// Verify OpenAPI spec dynamically reflects API_BASE_URL
	_ = os.Setenv("API_BASE_URL", "https://production.pipeline.io")
	_ = os.Setenv("API_BASE_PATH", "/api/v2")
	defer func() {
		_ = os.Unsetenv("API_BASE_URL")
		_ = os.Unsetenv("API_BASE_PATH")
	}()

	specReq := httptest.NewRequest(http.MethodGet, "/docs/openapi.json", nil)
	specRec := httptest.NewRecorder()
	srv.ServeHTTP(specRec, specReq)
	if specRec.Code != http.StatusOK {
		t.Fatalf("expected 200 for openapi spec, got %d", specRec.Code)
	}
	specBody := specRec.Body.String()
	if !strings.Contains(specBody, "https://production.pipeline.io") {
		t.Errorf("expected openapi spec to contain custom base url, got %s", specBody[:200])
	}
	if !strings.Contains(specBody, "/api/v2/pipelines") {
		t.Errorf("expected openapi spec to contain custom api base path /api/v2/pipelines, got %s", specBody[:200])
	}
}
