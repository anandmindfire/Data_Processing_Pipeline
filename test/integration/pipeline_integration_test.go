package test_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/controllers"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/validator"
	"github.com/anandmindfire/data-processing-pipeline/internal/bootstrap"
	"github.com/anandmindfire/data-processing-pipeline/internal/config"
	"github.com/anandmindfire/data-processing-pipeline/internal/export"
	"github.com/anandmindfire/data-processing-pipeline/internal/ingestion"
	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/pipeline"
	"github.com/anandmindfire/data-processing-pipeline/internal/services"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
	"github.com/anandmindfire/data-processing-pipeline/internal/transformation"
	"github.com/anandmindfire/data-processing-pipeline/internal/validation"
	"go.uber.org/goleak"
	_ "modernc.org/sqlite"
)

type mockTransport struct {
	roundTrip func(req *http.Request) (*http.Response, error)
}

func (m mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTrip(req)
}

func TestEndToEndPipelineIntegration(t *testing.T) {
	tempDir := t.TempDir()
	csvFile, jsonFile, exportJSON, exportCSV, exportSQL, deadLetterPath := prepareIntegrationFiles(t, tempDir)

	cfg := config.Load()
	cfg.MaxActiveJobs = 5
	repo := storage.NewMemoryRepository()
	collector := metrics.NewCollector()
	pipe := pipeline.NewPipeline(
		ingestion.NewReader(createMockHTTPClient()),
		validation.NewValidator(),
		transformation.NewTransformer(),
		export.NewExporter(tempDir),
		repo,
		collector,
	)
	service := services.NewPipelineService(cfg, repo, pipe)
	server, cleanupServer := newServer(cfg, service, tempDir)
	defer cleanupServer()

	jobSpec := buildIntegrationJobSpec(csvFile, jsonFile, exportJSON, exportCSV, exportSQL, deadLetterPath)
	jobID := submitIntegrationJob(t, server, jobSpec)

	pollIntegrationProgress(t, server, jobID)
	verifyIntegrationResults(t, server, jobID)
	verifyIntegrationExports(t, exportJSON, exportCSV, exportSQL, deadLetterPath)
	verifyIntegrationErrors(t, server, jobID)
	verifyIntegrationDelete(t, server, jobID)
}

func prepareIntegrationFiles(t *testing.T, tempDir string) (string, string, string, string, string, string) {
	t.Helper()
	csvFile := filepath.Join(tempDir, "input.csv")
	csvData := "name,department,salary\nAlice,Engineering,120000\nBob,Sales,90000\nCara,Engineering,135000\nDan,Sales,80000\nEve,Engineering,invalid\n"
	if err := os.WriteFile(csvFile, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	jsonFile := filepath.Join(tempDir, "input.json")
	jsonData := `[
  {"name":"Fay","department":"Engineering","salary":"110000"},
  {"name":"Gus","department":"Sales","salary":"95000"}
]`
	if err := os.WriteFile(jsonFile, []byte(jsonData), 0644); err != nil {
		t.Fatalf("failed to write test JSON: %v", err)
	}

	return csvFile, jsonFile,
		filepath.Join(tempDir, "out.json"),
		filepath.Join(tempDir, "out.csv"),
		filepath.Join(tempDir, "out.sql"),
		filepath.Join(tempDir, "dead_letter.json")
}

func createMockHTTPClient() *http.Client {
	return &http.Client{
		Transport: mockTransport{
			roundTrip: func(req *http.Request) (*http.Response, error) {
				if req.URL.Host == "api.company.internal" && req.URL.Path == "/contractors" {
					body := `[{"name":"Hank","department":"Engineering","salary":"105000"}]`
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       io.NopCloser(strings.NewReader(body)),
						Header:     make(http.Header),
					}, nil
				}
				return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader("not found"))}, nil
			},
		},
	}
}

func buildIntegrationJobSpec(csvFile, jsonFile, exportJSON, exportCSV, exportSQL, deadLetterPath string) models.Config {
	return models.Config{
		Sources: []models.Source{
			{Type: "csv", Name: "employees-csv", Path: csvFile},
			{Type: "json", Name: "employees-json", Path: jsonFile},
			{Type: "http", Name: "contractors-api", URL: "https://api.company.internal/contractors", Format: "json"},
		},
		Rules: []models.Rule{
			{Field: "name", Type: "string", Required: true, Normalize: "trim"},
			{Field: "department", Type: "string", Required: true, Normalize: "lower"},
			{Field: "salary", Type: "number", Required: true, Min: ptr(0.0)},
		},
		Aggregations: []models.Aggregation{
			{GroupBy: "department", Field: "salary", Operation: "count", Name: "employee_count"},
			{GroupBy: "department", Field: "salary", Operation: "sum", Name: "salary_total"},
			{GroupBy: "department", Field: "salary", Operation: "avg", Name: "salary_average"},
		},
		Exports: []models.ExportTarget{
			{Type: "json", Path: exportJSON},
			{Type: "csv", Path: exportCSV},
			{Type: "sql", Path: exportSQL},
			{Type: "sqlite", Path: strings.TrimSuffix(exportSQL, ".sql") + ".db"},
		},
		DLQPath:           deadLetterPath,
		ValidationWorkers: 4,
		TransformWorkers:  4,
		BufferSize:        32,
		TimeoutSeconds:    10,
	}
}

func submitIntegrationJob(t *testing.T, server http.Handler, spec models.Config) string {
	t.Helper()
	bodyBytes, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("failed to marshal job spec: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(bodyBytes))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected HTTP 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
	}

	var createdJob models.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &createdJob); err != nil {
		t.Fatalf("failed to parse created job: %v", err)
	}
	if createdJob.ID == "" {
		t.Fatal("expected non-empty job ID")
	}
	return createdJob.ID
}

func pollIntegrationProgress(t *testing.T, server http.Handler, jobID string) {
	t.Helper()
	for i := 0; i < 40; i++ {
		time.Sleep(50 * time.Millisecond)
		progReq := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID+"/progress", nil)
		progRec := httptest.NewRecorder()
		server.ServeHTTP(progRec, progReq)

		if progRec.Code != http.StatusOK {
			t.Fatalf("progress failed with code %d", progRec.Code)
		}

		var progress models.Progress
		_ = json.Unmarshal(progRec.Body.Bytes(), &progress)
		if progress.Status == models.StatusCompleted {
			if progress.Processed != 7 || progress.Rejected != 1 {
				t.Fatalf("unexpected progress counts: processed=%d, rejected=%d", progress.Processed, progress.Rejected)
			}
			return
		}
	}
	t.Fatal("pipeline job did not complete within allotted timeout")
}

func verifyIntegrationResults(t *testing.T, server http.Handler, jobID string) {
	t.Helper()
	resReq := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID+"/results", nil)
	resRec := httptest.NewRecorder()
	server.ServeHTTP(resRec, resReq)

	if resRec.Code != http.StatusOK {
		t.Fatalf("results failed with code %d", resRec.Code)
	}

	var results struct {
		JobID        string                    `json:"job_id"`
		Status       models.JobStatus          `json:"status"`
		Records      []models.Record           `json:"records"`
		Aggregations map[string]map[string]any `json:"aggregations"`
		ExportPath   string                    `json:"export_path"`
	}
	if err := json.Unmarshal(resRec.Body.Bytes(), &results); err != nil {
		t.Fatalf("failed to decode results: %v", err)
	}

	if len(results.Records) != 7 {
		t.Fatalf("expected 7 valid records in results, got %d", len(results.Records))
	}

	counts := results.Aggregations["employee_count"]
	if fmt.Sprint(counts["engineering"]) != "4" || fmt.Sprint(counts["sales"]) != "3" {
		t.Errorf("unexpected counts: %v", counts)
	}

	sums := results.Aggregations["salary_total"]
	if fmt.Sprint(sums["engineering"]) != "470000" || fmt.Sprint(sums["sales"]) != "265000" {
		t.Errorf("unexpected sums: %v", sums)
	}
}

func verifyIntegrationExports(t *testing.T, exportJSON, exportCSV, exportSQL, deadLetterPath string) {
	t.Helper()
	verifyJSONExportFile(t, exportJSON, 7)
	verifyCSVExportFile(t, exportCSV, 8)
	verifySQLExportFile(t, exportSQL)
	verifySQLiteDatabase(t, strings.TrimSuffix(exportSQL, ".sql")+".db", 7)
	verifyDeadLetterFile(t, deadLetterPath, "Eve")
}

// verifySQLiteDatabase opens the exported database through database/sql and checks its contents.
func verifySQLiteDatabase(t *testing.T, path string, expectedRecords int) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM records`).Scan(&n); err != nil || n != expectedRecords {
		t.Fatalf("expected %d rows in sqlite records table, got %d (err: %v)", expectedRecords, n, err)
	}
	var aggs int
	if err := db.QueryRow(`SELECT COUNT(*) FROM aggregations`).Scan(&aggs); err != nil || aggs == 0 {
		t.Fatalf("expected aggregations table rows, got %d (err: %v)", aggs, err)
	}
}

// newServer wires the production router around a custom service, sandboxed to dir.
func newServer(cfg *config.Config, service *services.PipelineService, dir string) (http.Handler, func()) {
	ctrl := controllers.NewPipelineController(service, cfg.MaxRequestBodyBytes, validator.DefaultPolicy(dir, dir))
	deps := &bootstrap.Dependencies{Config: cfg, Service: service, Controller: ctrl}
	router := bootstrap.BuildRouter(deps)
	cleanup := func() {
		if deps.Limiter != nil {
			deps.Limiter.Close()
		}
	}
	return router, cleanup
}

func verifyJSONExportFile(t *testing.T, path string, expectedCount int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read export JSON: %v", err)
	}
	var records []map[string]any
	if err := json.Unmarshal(data, &records); err != nil || len(records) != expectedCount {
		t.Fatalf("expected %d records in JSON, got %d (err: %v)", expectedCount, len(records), err)
	}
}

func verifyCSVExportFile(t *testing.T, path string, expectedRows int) {
	t.Helper()
	fd, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open export CSV: %v", err)
	}
	defer fd.Close()

	rows, err := csv.NewReader(fd).ReadAll()
	if err != nil || len(rows) != expectedRows {
		t.Fatalf("expected %d CSV rows, got %d (err: %v)", expectedRows, len(rows), err)
	}
}

func verifySQLExportFile(t *testing.T, path string) {
	t.Helper()
	sqlBytes, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(sqlBytes), "CREATE TABLE IF NOT EXISTS records") {
		t.Fatalf("expected valid CREATE TABLE in SQLite dump")
	}
}

func verifyDeadLetterFile(t *testing.T, path, expectedContent string) {
	t.Helper()
	dlBytes, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(dlBytes), expectedContent) {
		t.Fatalf("expected dead-letter file to contain %q", expectedContent)
	}
}

func verifyIntegrationErrors(t *testing.T, server http.Handler, jobID string) {
	t.Helper()
	errReq := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID+"/errors", nil)
	errRec := httptest.NewRecorder()
	server.ServeHTTP(errRec, errReq)

	if errRec.Code != http.StatusOK {
		t.Fatalf("errors failed with code %d", errRec.Code)
	}

	var errorsResp struct {
		JobID  string               `json:"job_id"`
		Errors []models.RecordError `json:"errors"`
	}
	if err := json.Unmarshal(errRec.Body.Bytes(), &errorsResp); err != nil {
		t.Fatalf("failed to decode errors response: %v", err)
	}

	if len(errorsResp.Errors) != 1 {
		t.Fatalf("expected 1 quarantined error, got %d", len(errorsResp.Errors))
	}
	badRow := errorsResp.Errors[0]
	if badRow.Stage != "validation" || badRow.Record["name"] != "Eve" {
		t.Errorf("unexpected bad row: %+v", badRow)
	}
}

func verifyIntegrationDelete(t *testing.T, server http.Handler, jobID string) {
	t.Helper()
	delReq := httptest.NewRequest(http.MethodDelete, "/api/v1/pipelines/"+jobID, nil)
	delRec := httptest.NewRecorder()
	server.ServeHTTP(delRec, delReq)
	if delRec.Code != http.StatusNoContent {
		t.Fatalf("expected HTTP 204 No Content for delete, got %d", delRec.Code)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID, nil)
	getRec := httptest.NewRecorder()
	server.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusNotFound {
		t.Fatalf("expected HTTP 404 after deletion, got %d", getRec.Code)
	}
}

func TestPipelineCancellation(t *testing.T) {
	defer goleak.VerifyNone(t)

	tempDir := t.TempDir()
	cfg := config.Load()
	repo := storage.NewMemoryRepository()
	defer func() { _ = repo.Close() }()
	collector := metrics.NewCollector()

	// Slow reader that delays before returning records to allow cancellation test
	slowClient := &http.Client{
		Transport: mockTransport{
			roundTrip: func(req *http.Request) (*http.Response, error) {
				select {
				case <-time.After(5 * time.Second):
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       io.NopCloser(strings.NewReader(`[{"name":"Slow","val":1}]`)),
						Header:     make(http.Header),
					}, nil
				case <-req.Context().Done():
					return nil, req.Context().Err()
				}
			},
		},
	}

	pipe := pipeline.NewPipeline(
		ingestion.NewReader(slowClient),
		validation.NewValidator(),
		transformation.NewTransformer(),
		export.NewExporter(tempDir),
		repo,
		collector,
	)
	service := services.NewPipelineService(cfg, repo, pipe)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = service.Shutdown(ctx)
	}()

	server, cleanupServer := newServer(cfg, service, tempDir)
	defer cleanupServer()

	jobSpec := models.Config{
		Sources: []models.Source{
			{Type: "http", Name: "slow-source", URL: "https://api.company.internal/slow", Format: "json"},
		},
		Exports: []models.ExportTarget{
			{Type: "json", Path: filepath.Join(tempDir, "cancelled.json")},
		},
		TimeoutSeconds: 30,
	}

	body, _ := json.Marshal(jobSpec)
	postReq := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(body))
	postRec := httptest.NewRecorder()
	server.ServeHTTP(postRec, postReq)

	var job models.Job
	_ = json.Unmarshal(postRec.Body.Bytes(), &job)

	// Cancel the job immediately
	cancelReq := httptest.NewRequest(http.MethodPatch, "/api/v1/pipelines/"+job.ID+"/cancel", nil)
	cancelRec := httptest.NewRecorder()
	server.ServeHTTP(cancelRec, cancelReq)

	if cancelRec.Code != http.StatusAccepted {
		t.Fatalf("expected HTTP 202 for cancellation, got %d", cancelRec.Code)
	}

	// Poll until context cancellation propagates
	cancelled := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		statusReq := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+job.ID, nil)
		statusRec := httptest.NewRecorder()
		server.ServeHTTP(statusRec, statusReq)

		var updatedJob models.Job
		_ = json.Unmarshal(statusRec.Body.Bytes(), &updatedJob)
		if updatedJob.Status == models.StatusCancelled || updatedJob.Status == models.StatusFailed {
			cancelled = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !cancelled {
		t.Fatalf("job was not cancelled within timeout")
	}
}

func TestPipelineMidRunProgress(t *testing.T) {
	tempDir := t.TempDir()
	cfg := config.Load()
	repo := storage.NewMemoryRepository()
	defer func() { _ = repo.Close() }()
	collector := metrics.NewCollector()

	proceed := make(chan struct{})
	gateClient := &http.Client{
		Transport: mockTransport{
			roundTrip: func(req *http.Request) (*http.Response, error) {
				select {
				case <-proceed:
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       io.NopCloser(strings.NewReader(`[{"name":"Alice","val":1},{"name":"Bob","val":2}]`)),
						Header:     make(http.Header),
					}, nil
				case <-req.Context().Done():
					return nil, req.Context().Err()
				}
			},
		},
	}

	pipe := pipeline.NewPipeline(
		ingestion.NewReader(gateClient),
		validation.NewValidator(),
		transformation.NewTransformer(),
		export.NewExporter(tempDir),
		repo,
		collector,
	)
	service := services.NewPipelineService(cfg, repo, pipe)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = service.Shutdown(ctx)
	}()

	server, cleanupServer := newServer(cfg, service, tempDir)
	defer cleanupServer()

	jobSpec := models.Config{
		Sources: []models.Source{
			{Type: "http", Name: "gate-source", URL: "https://api.company.internal/gate", Format: "json"},
		},
		Exports: []models.ExportTarget{
			{Type: "json", Path: filepath.Join(tempDir, "progress_out.json")},
		},
		TimeoutSeconds: 30,
	}

	body, _ := json.Marshal(jobSpec)
	postReq := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(body))
	postRec := httptest.NewRecorder()
	server.ServeHTTP(postRec, postReq)

	var job models.Job
	_ = json.Unmarshal(postRec.Body.Bytes(), &job)

	// Verify mid-run progress polling while ingestion is paused
	progReq := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+job.ID+"/progress", nil)
	progRec := httptest.NewRecorder()
	server.ServeHTTP(progRec, progReq)

	if progRec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for progress mid-run, got %d: %s", progRec.Code, progRec.Body.String())
	}

	var midProgress models.Progress
	if err := json.Unmarshal(progRec.Body.Bytes(), &midProgress); err != nil {
		t.Fatalf("failed to parse mid-run progress JSON: %v", err)
	}
	if midProgress.JobID != job.ID {
		t.Errorf("expected job ID %s, got %s", job.ID, midProgress.JobID)
	}
	if midProgress.Status != models.StatusRunning && midProgress.Status != models.StatusPending {
		t.Errorf("expected running or pending mid-run status, got %s", midProgress.Status)
	}

	// Release ingestion and allow pipeline to finish
	close(proceed)

	deadline := time.Now().Add(3 * time.Second)
	completed := false
	for time.Now().Before(deadline) {
		statusReq := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+job.ID, nil)
		statusRec := httptest.NewRecorder()
		server.ServeHTTP(statusRec, statusReq)

		var updatedJob models.Job
		_ = json.Unmarshal(statusRec.Body.Bytes(), &updatedJob)
		if updatedJob.Status == models.StatusCompleted {
			completed = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !completed {
		t.Fatalf("job did not complete after releasing gate")
	}

	// Final progress check
	finalProgReq := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+job.ID+"/progress", nil)
	finalProgRec := httptest.NewRecorder()
	server.ServeHTTP(finalProgRec, finalProgReq)
	if finalProgRec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for final progress, got %d", finalProgRec.Code)
	}
	var finalProgress models.Progress
	_ = json.Unmarshal(finalProgRec.Body.Bytes(), &finalProgress)
	if finalProgress.Status != models.StatusCompleted {
		t.Errorf("expected completed final progress, got %s", finalProgress.Status)
	}
	if finalProgress.Processed != 2 {
		t.Errorf("expected 2 processed records, got %d", finalProgress.Processed)
	}
}

func TestPipelineFailedIngestion(t *testing.T) {
	tempDir := t.TempDir()
	cfg := config.Load()
	cfg.DataDir = filepath.Join(tempDir, "data")
	cfg.ExportDir = filepath.Join(tempDir, "exports")

	repo, err := storage.NewSQLiteRepository(context.Background(), cfg.DataDir)
	if err != nil {
		t.Fatalf("failed to create repo: %v", err)
	}
	defer repo.Close()

	collector := metrics.NewCollector()
	pipe := pipeline.NewPipeline(
		ingestion.NewReader(http.DefaultClient),
		validation.NewValidator(),
		transformation.NewTransformer(),
		export.NewExporter(tempDir),
		repo,
		collector,
	)
	service := services.NewPipelineService(cfg, repo, pipe)
	server, cleanupServer := newServer(cfg, service, tempDir)
	defer cleanupServer()

	jobSpec := models.Config{
		Sources: []models.Source{
			{Type: "csv", Name: "nonexistent-file", Path: filepath.Join(tempDir, "does_not_exist.csv")},
		},
		Exports: []models.ExportTarget{
			{Type: "json", Path: filepath.Join(tempDir, "failed.json")},
		},
		TimeoutSeconds: 5,
	}

	body, _ := json.Marshal(jobSpec)
	postReq := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(body))
	postRec := httptest.NewRecorder()
	server.ServeHTTP(postRec, postReq)

	var job models.Job
	_ = json.Unmarshal(postRec.Body.Bytes(), &job)

	// Poll until job completes with FAILED status
	failed := false
	for i := 0; i < 30; i++ {
		time.Sleep(20 * time.Millisecond)
		statusReq := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+job.ID, nil)
		statusRec := httptest.NewRecorder()
		server.ServeHTTP(statusRec, statusReq)

		var updatedJob models.Job
		_ = json.Unmarshal(statusRec.Body.Bytes(), &updatedJob)
		if updatedJob.Status == models.StatusFailed {
			failed = true
			break
		}
	}
	if !failed {
		t.Fatalf("expected job to fail due to missing source file, status remained non-failed")
	}

	// Verify error endpoint lists the ingestion failure
	errReq := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+job.ID+"/errors", nil)
	errRec := httptest.NewRecorder()
	server.ServeHTTP(errRec, errReq)

	var errResp struct {
		JobID  string               `json:"job_id"`
		Errors []models.RecordError `json:"errors"`
	}
	_ = json.Unmarshal(errRec.Body.Bytes(), &errResp)
	if len(errResp.Errors) == 0 {
		t.Fatalf("expected at least one error recorded for nonexistent source")
	}
	if errResp.Errors[0].Stage != "ingest" {
		t.Fatalf("expected stage 'ingest', got %q", errResp.Errors[0].Stage)
	}
}

func ptr(v float64) *float64 {
	return &v
}
