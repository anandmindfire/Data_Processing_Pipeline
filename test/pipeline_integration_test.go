package test_test

import (
	"bytes"
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

	"github.com/anandmindfire/data-processing-pipeline/internal/api"
	"github.com/anandmindfire/data-processing-pipeline/internal/config"
	"github.com/anandmindfire/data-processing-pipeline/internal/export"
	"github.com/anandmindfire/data-processing-pipeline/internal/ingestion"
	"github.com/anandmindfire/data-processing-pipeline/internal/jobs"
	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/pipeline"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
	"github.com/anandmindfire/data-processing-pipeline/internal/transformation"
	"github.com/anandmindfire/data-processing-pipeline/internal/validation"
)

type mockTransport struct {
	roundTrip func(req *http.Request) (*http.Response, error)
}

func (m mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTrip(req)
}

func TestEndToEndPipelineIntegration(t *testing.T) {
	tempDir := t.TempDir()

	// 1. Prepare sample input files
	csvFile := filepath.Join(tempDir, "input.csv")
	csvData := `name,department,salary
Alice,Engineering,120000
Bob,Sales,90000
Cara,Engineering,135000
Dan,Sales,80000
Eve,Engineering,invalid
`
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

	exportJSON := filepath.Join(tempDir, "out.json")
	exportCSV := filepath.Join(tempDir, "out.csv")

	// 2. Mock HTTP endpoint for remote API ingestion
	mockClient := &http.Client{
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

	// 3. Assemble server stack
	cfg := config.Load()
	cfg.MaxActiveJobs = 5
	repo := storage.NewMemoryRepository()
	collector := metrics.NewCollector()
	pipe := pipeline.NewPipeline(
		ingestion.NewReader(mockClient),
		validation.NewValidator(),
		transformation.NewTransformer(),
		export.NewExporter(tempDir),
		repo,
		collector,
	)
	mgr := jobs.NewManager(cfg, repo, pipe)
	server := api.NewServer(cfg, mgr)

	// 4. Submit pipeline job via POST /api/v1/pipelines
	jobSpec := models.Config{
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
		},
		ValidationWorkers: 4,
		TransformWorkers:  4,
		BufferSize:        32,
		TimeoutSeconds:    10,
	}

	bodyBytes, err := json.Marshal(jobSpec)
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
	jobID := createdJob.ID
	if jobID == "" {
		t.Fatal("expected non-empty job ID")
	}

	// 5. Poll for completion via GET /api/v1/pipelines/:id/progress
	completed := false
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
			completed = true
			if progress.Processed != 7 {
				t.Fatalf("expected 7 processed records (4 csv + 2 json + 1 http), got %d", progress.Processed)
			}
			if progress.Rejected != 1 {
				t.Fatalf("expected 1 rejected record (invalid salary), got %d", progress.Rejected)
			}
			break
		}
	}

	if !completed {
		t.Fatal("pipeline job did not complete within allotted timeout")
	}

	// 6. Verify Results via GET /api/v1/pipelines/:id/results
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

	// Assert aggregations:
	// Engineering: Alice (120000), Cara (135000), Fay (110000), Hank (105000) -> count=4, sum=470000, avg=117500
	// Sales: Bob (90000), Dan (80000), Gus (95000) -> count=3, sum=265000, avg=88333.33...
	counts := results.Aggregations["employee_count"]
	if fmt.Sprint(counts["engineering"]) != "4" {
		t.Errorf("expected 4 engineering employees, got %v", counts["engineering"])
	}
	if fmt.Sprint(counts["sales"]) != "3" {
		t.Errorf("expected 3 sales employees, got %v", counts["sales"])
	}

	sums := results.Aggregations["salary_total"]
	if fmt.Sprint(sums["engineering"]) != "470000" {
		t.Errorf("expected 470000 engineering total salary, got %v", sums["engineering"])
	}
	if fmt.Sprint(sums["sales"]) != "265000" {
		t.Errorf("expected 265000 sales total salary, got %v", sums["sales"])
	}

	// 7. Verify exported files on disk
	if _, err := os.Stat(exportJSON); os.IsNotExist(err) {
		t.Fatalf("expected export JSON file %s to exist", exportJSON)
	}
	exportedJSONBytes, err := os.ReadFile(exportJSON)
	if err != nil {
		t.Fatalf("failed to read export JSON: %v", err)
	}
	var exportedJSONRecords []map[string]any
	if err := json.Unmarshal(exportedJSONBytes, &exportedJSONRecords); err != nil {
		t.Fatalf("failed to parse exported JSON: %v", err)
	}
	if len(exportedJSONRecords) != 7 {
		t.Fatalf("expected 7 records in exported JSON, got %d", len(exportedJSONRecords))
	}

	if _, err := os.Stat(exportCSV); os.IsNotExist(err) {
		t.Fatalf("expected export CSV file %s to exist", exportCSV)
	}
	csvFd, err := os.Open(exportCSV)
	if err != nil {
		t.Fatalf("failed to open export CSV: %v", err)
	}
	defer csvFd.Close()
	csvRows, err := csv.NewReader(csvFd).ReadAll()
	if err != nil {
		t.Fatalf("failed to read export CSV: %v", err)
	}
	if len(csvRows) != 8 { // 1 header + 7 rows
		t.Fatalf("expected 8 CSV rows (1 header + 7 data), got %d", len(csvRows))
	}

	// 8. Verify Error Quarantine via GET /api/v1/pipelines/:id/errors
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
	if badRow.Stage != "validation" {
		t.Errorf("expected error stage 'validation', got %s", badRow.Stage)
	}
	if badRow.Record["name"] != "Eve" {
		t.Errorf("expected failed record for 'Eve', got %v", badRow.Record)
	}

	// 9. Verify DELETE endpoint deletes job
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
	tempDir := t.TempDir()
	cfg := config.Load()
	repo := storage.NewMemoryRepository()
	collector := metrics.NewCollector()

	// Slow reader that delays before returning records to allow cancellation test
	slowClient := &http.Client{
		Transport: mockTransport{
			roundTrip: func(req *http.Request) (*http.Response, error) {
				select {
				case <-time.After(2 * time.Second):
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
	mgr := jobs.NewManager(cfg, repo, pipe)
	server := api.NewServer(cfg, mgr)

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
	for i := 0; i < 30; i++ {
		time.Sleep(20 * time.Millisecond)
		statusReq := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+job.ID, nil)
		statusRec := httptest.NewRecorder()
		server.ServeHTTP(statusRec, statusReq)

		var updatedJob models.Job
		_ = json.Unmarshal(statusRec.Body.Bytes(), &updatedJob)
		if updatedJob.Status == models.StatusCancelled || updatedJob.Status == models.StatusFailed {
			cancelled = true
			break
		}
	}
	if !cancelled {
		t.Fatalf("job was not cancelled within timeout")
	}
}

func ptr(v float64) *float64 {
	return &v
}
