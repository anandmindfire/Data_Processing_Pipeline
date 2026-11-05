package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/config"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

// testConfig returns a config whose data, export, and input directories live in a temp dir.
func testConfig(t *testing.T) (*config.Config, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.ExportDir = filepath.Join(dir, "exports")
	cfg.InputDirs = []string{filepath.Join(dir, "inputs")}
	if err := os.MkdirAll(cfg.InputDirs[0], 0o755); err != nil {
		t.Fatal(err)
	}
	return cfg, dir
}

func newTestServer(t *testing.T, cfg *config.Config) (http.Handler, *Dependencies) {
	t.Helper()
	deps, err := BuildDependencies(context.Background(), cfg)
	if err != nil {
		t.Fatalf("build dependencies: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = deps.Service.Shutdown(ctx)
		_ = deps.Store.Close()
		if deps.Limiter != nil {
			deps.Limiter.Close()
		}
	})
	return BuildRouter(deps), deps
}

func setupTestServer(t *testing.T) (http.Handler, storage.Repository, *config.Config) {
	t.Helper()
	cfg, _ := testConfig(t)
	srv, deps := newTestServer(t, cfg)
	return srv, deps.Store, cfg
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
	srv, store, cfg := setupTestServer(t)

	inputPath := filepath.Join(cfg.InputDirs[0], "data.csv")
	if err := os.WriteFile(inputPath, []byte("name,salary\nAlice,100\nBob,bad\n"), 0644); err != nil {
		t.Fatal(err)
	}
	outputPath := "out.json" // relative paths resolve inside the export directory

	spec := models.JobSpec{
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

	body, _ := json.Marshal(spec)

	jobID := testCreateAndListPipeline(t, srv, body)
	waitForJobCompletion(t, store, jobID)
	testJobDetailsAndProgress(t, srv, jobID)
	testJobResultsAndErrors(t, srv, jobID)

	exported := filepath.Join(cfg.ExportDir, "out.json")
	if _, err := os.Stat(exported); err != nil {
		t.Fatalf("expected export inside export dir: %v", err)
	}
	testJobCancelAndDeletion(t, srv, jobID)
	if _, err := os.Stat(exported); !os.IsNotExist(err) {
		t.Fatalf("expected delete to remove exported artifact, stat err=%v", err)
	}
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
	if listRec.Header().Get("X-Total-Count") != "1" {
		t.Fatalf("expected X-Total-Count=1, got %q", listRec.Header().Get("X-Total-Count"))
	}

	return createdJob.ID
}

func waitForJobCompletion(t *testing.T, store storage.Repository, jobID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j, ok, _ := store.GetJob(context.Background(), jobID)
		if ok && j.Status.IsTerminal() {
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
	cfgEmptySources := models.JobSpec{Exports: []models.ExportTarget{{Type: "json"}}}
	b, _ := json.Marshal(cfgEmptySources)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(b))
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty sources, got %d", rec.Code)
	}

	// Empty exports
	cfgEmptyExports := models.JobSpec{Sources: []models.Source{{Type: "csv", Path: "in.csv"}}}
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
	cfg, _ := testConfig(t)
	cfg.APIBasePath = "/api/v2"
	cfg.APIBaseURL = "https://production.pipeline.io"
	srv, _ := newTestServer(t, cfg)

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

func postJob(t *testing.T, srv http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", strings.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

func TestJobSpecSecurityAndLimits(t *testing.T) {
	srv, _, cfg := setupTestServer(t)
	in := filepath.Join(cfg.InputDirs[0], "in.csv")
	if err := os.WriteFile(in, []byte("a\n1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	src := `{"type":"csv","name":"s","path":` + strconv.Quote(in) + `}`

	cases := map[string]string{
		"source outside input dirs": `{"sources":[{"type":"csv","name":"s","path":"/etc/hosts"}],"exports":[{"type":"json"}]}`,
		"source path traversal":     `{"sources":[{"type":"csv","name":"s","path":` + strconv.Quote(filepath.Join(cfg.InputDirs[0], "..", "..", "etc", "hosts")) + `}],"exports":[{"type":"json"}]}`,
		"export outside export dir": `{"sources":[` + src + `],"exports":[{"type":"json","path":"/tmp/evil.json"}]}`,
		"export traversal":          `{"sources":[` + src + `],"exports":[{"type":"json","path":"../../evil.json"}]}`,
		"dlq outside export dir":    `{"sources":[` + src + `],"exports":[{"type":"json"}],"dlq_path":"/tmp/dlq.json"}`,
		"too many workers":          `{"sources":[` + src + `],"exports":[{"type":"json"}],"validation_workers":2000000}`,
		"oversized buffer":          `{"sources":[` + src + `],"exports":[{"type":"json"}],"buffer_size":100000000}`,
		"timeout above max":         `{"sources":[` + src + `],"exports":[{"type":"json"}],"timeout_seconds":999999}`,
		"unknown export type":       `{"sources":[` + src + `],"exports":[{"type":"parquet"}]}`,
		"unknown source type":       `{"sources":[{"type":"xml","name":"s","path":` + strconv.Quote(in) + `}],"exports":[{"type":"json"}]}`,
		"unknown aggregation op":    `{"sources":[` + src + `],"exports":[{"type":"json"}],"aggregations":[{"field":"a","operation":"median"}]}`,
		"invalid regex":             `{"sources":[` + src + `],"exports":[{"type":"json"}],"rules":[{"field":"a","pattern":"("}]}`,
		"duplicate export paths":    `{"sources":[` + src + `],"exports":[{"type":"json","path":"x.json"},{"type":"json","path":"x.json"}]}`,
		"non-http url":              `{"sources":[{"type":"http","name":"s","url":"file:///etc/passwd"}],"exports":[{"type":"json"}]}`,
		"http source without url":   `{"sources":[{"type":"http","name":"s","path":` + strconv.Quote(in) + `}],"exports":[{"type":"json"}]}`,
		"negative workers":          `{"sources":[` + src + `],"exports":[{"type":"json"}],"transform_workers":-1}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if rec := postJob(t, srv, body); rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
			}
		})
	}

	// Server-side defaults are applied to the stored job.
	rec := postJob(t, srv, `{"sources":[`+src+`],"exports":[{"type":"json"}]}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}
	var job models.Job
	_ = json.Unmarshal(rec.Body.Bytes(), &job)
	if job.Spec.ValidationWorkers != cfg.ValidationWorkers || job.Spec.TimeoutSeconds != cfg.DefaultTimeoutSeconds {
		t.Fatalf("expected service defaults to be applied, got %+v", job.Spec)
	}
}

func TestPaginationParameters(t *testing.T) {
	srv, _, _ := setupTestServer(t)
	for _, q := range []string{"?limit=0", "?limit=5000", "?offset=-1", "?limit=abc", "?status=bogus"} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines"+q, nil)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: expected 400, got %d", q, rec.Code)
		}
	}
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/pipelines/does-not-exist/cancel", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 cancelling unknown job, got %d", rec.Code)
	}
}

func TestRestartMarksInterruptedJobsFailed(t *testing.T) {
	cfg, _ := testConfig(t)
	store, err := storage.NewSQLiteRepository(context.Background(), cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for _, j := range []*models.Job{
		{ID: "was-running", Status: models.StatusRunning, CreatedAt: now, UpdatedAt: now},
		{ID: "was-pending", Status: models.StatusPending, CreatedAt: now, UpdatedAt: now},
		{ID: "done", Status: models.StatusCompleted, CreatedAt: now, UpdatedAt: now},
	} {
		if err := store.SaveJob(context.Background(), j); err != nil {
			t.Fatal(err)
		}
	}
	_ = store.Close()

	_, deps := newTestServer(t, cfg)
	for id, want := range map[string]models.JobStatus{"was-running": models.StatusFailed, "was-pending": models.StatusFailed, "done": models.StatusCompleted} {
		j, _, _ := deps.Store.GetJob(context.Background(), id)
		if j.Status != want {
			t.Fatalf("%s: expected %s, got %s", id, want, j.Status)
		}
		if want == models.StatusFailed && j.Error == "" {
			t.Fatalf("%s: expected an interruption reason", id)
		}
	}
}

func TestShutdownCancelsRunningJobs(t *testing.T) {
	cfg, _ := testConfig(t)
	in := filepath.Join(cfg.InputDirs[0], "big.csv")
	var sb strings.Builder
	sb.WriteString("id,v\n")
	for i := 0; i < 200000; i++ {
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(",x\n")
	}
	if err := os.WriteFile(in, []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	srv, deps := newTestServer(t, cfg)
	rec := postJob(t, srv, `{"sources":[{"type":"csv","name":"s","path":`+strconv.Quote(in)+`}],"exports":[{"type":"json"}],"validation_workers":1,"transform_workers":1,"buffer_size":1}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	var job models.Job
	_ = json.Unmarshal(rec.Body.Bytes(), &job)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := deps.Service.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	got, _, _ := deps.Store.GetJob(context.Background(), job.ID)
	if got.Status != models.StatusCancelled || !strings.Contains(got.Error, "shutting down") {
		t.Fatalf("expected CANCELLED by shutdown, got %s %q", got.Status, got.Error)
	}
	if rec := postJob(t, srv, `{}`); rec.Code != http.StatusBadRequest && rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status after shutdown: %d", rec.Code)
	}
}

func TestEdgeHardening(t *testing.T) {
	cfg, _ := testConfig(t)
	cfg.MaxRequestBodyBytes = 512
	srv, _ := newTestServer(t, cfg)
	do := func(method, path, ctype, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if ctype != "" {
			req.Header.Set("Content-Type", ctype)
		}
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		return rec
	}

	if rec := do(http.MethodGet, "/readyz", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("readyz: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(http.MethodGet, "/healthz", "", ""); !strings.Contains(rec.Body.String(), `"version"`) {
		t.Fatalf("healthz should report build info: %s", rec.Body.String())
	}
	if rec := do(http.MethodPost, "/api/v1/pipelines", "application/json", `{"sources":[],"exports":[],"validation_worker":4}`); rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "unknown field") {
		t.Fatalf("unknown fields must be rejected: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(http.MethodPost, "/api/v1/pipelines", "application/json", `{} {}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("trailing data must be rejected: %d", rec.Code)
	}
	if rec := do(http.MethodPost, "/api/v1/pipelines", "text/plain", `{}`); rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("expected 415, got %d", rec.Code)
	}
	if rec := do(http.MethodPost, "/api/v1/pipelines", "application/json", `{"sources":"`+strings.Repeat("x", 1024)+`"}`); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rec.Code)
	}
	docs := do(http.MethodGet, "/docs", "", "")
	if csp := docs.Header().Get("Content-Security-Policy"); !strings.Contains(csp, "'sha256-") || strings.Contains(csp, "unsafe-inline") {
		t.Fatalf("docs CSP should whitelist inline code by hash only: %q", csp)
	}
	if !strings.Contains(docs.Body.String(), `integrity="sha384-`) {
		t.Fatal("docs assets must carry Subresource Integrity hashes")
	}

	cfg2, _ := testConfig(t)
	cfg2.DocsEnabled = false
	cfg2.APIKey = "a-very-long-test-api-key"
	srv2, _ := newTestServer(t, cfg2)
	for path, want := range map[string]int{"/docs": http.StatusNotFound, "/metrics": http.StatusUnauthorized, "/healthz": http.StatusOK, "/api/v1/pipelines": http.StatusUnauthorized} {
		rec := httptest.NewRecorder()
		srv2.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != want {
			t.Fatalf("%s: expected %d, got %d", path, want, rec.Code)
		}
	}
}

func TestAPIKeyManagementFlow(t *testing.T) {
	cfg, _ := testConfig(t)
	const root = "root-admin-key-0123456789"
	cfg.APIKey = root
	srv, _ := newTestServer(t, cfg)

	call := func(method, path, key, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		return rec
	}

	// The admin (root) key issues an expiring key; the secret is returned once.
	rec := call(http.MethodPost, "/api/v1/auth/keys", root, `{"name":"ci-bot","expires_in":"2h"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create key: %d %s", rec.Code, rec.Body.String())
	}
	var created struct {
		ID, Key, Status, Prefix string
		ExpiresAt               time.Time `json:"expires_at"`
		CreatedAt               time.Time `json:"created_at"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &created)
	if !strings.HasPrefix(created.Key, "dpp_") || created.Status != "active" || created.ExpiresAt.Sub(created.CreatedAt) != 2*time.Hour {
		t.Fatalf("unexpected created key: %+v", created)
	}

	// The generated key reaches every pipeline API ...
	for _, path := range []string{"/api/v1/pipelines", "/metrics"} {
		if rec := call(http.MethodGet, path, created.Key, ""); rec.Code != http.StatusOK {
			t.Fatalf("generated key on %s: %d %s", path, rec.Code, rec.Body.String())
		}
	}
	// ... but cannot manage keys (it could otherwise mint keys that outlive it).
	if rec := call(http.MethodPost, "/api/v1/auth/keys", created.Key, `{"name":"escalate"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("generated key must not create keys: %d", rec.Code)
	}

	// Listing never exposes secrets.
	rec = call(http.MethodGet, "/api/v1/auth/keys", root, "")
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), created.Key) || !strings.Contains(rec.Body.String(), created.Prefix) {
		t.Fatalf("list keys: %d %s", rec.Code, rec.Body.String())
	}

	// Validation errors.
	for _, body := range []string{
		`{"name":"x","expires_in":"10s"}`,
		`{"name":"x","expires_in":"99999h"}`,
		`{"name":"x","expires_in":"1h","expires_at":"2030-01-01T00:00:00Z"}`,
		`{"name":"x","expires_at":"2000-01-01T00:00:00Z"}`,
		`{"name":"","expires_in":"1h"}`,
		`{"name":"x","scope":"admin"}`,
	} {
		if rec := call(http.MethodPost, "/api/v1/auth/keys", root, body); rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: expected 400, got %d %s", body, rec.Code, rec.Body.String())
		}
	}

	// Revocation takes effect immediately on this instance.
	if rec := call(http.MethodDelete, "/api/v1/auth/keys/"+created.ID, root, ""); rec.Code != http.StatusNoContent {
		t.Fatalf("revoke: %d", rec.Code)
	}
	if rec := call(http.MethodGet, "/api/v1/pipelines", created.Key, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("revoked key must be rejected, got %d", rec.Code)
	}
	if rec := call(http.MethodDelete, "/api/v1/auth/keys/key_missing", root, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("revoke missing: %d", rec.Code)
	}

	// Without API_KEY there is no admin, so key management is refused.
	cfg2, _ := testConfig(t)
	srv2, _ := newTestServer(t, cfg2)
	rec = httptest.NewRecorder()
	srv2.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/auth/keys", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 when API_KEY is unset, got %d", rec.Code)
	}
}

// TestPostgresBackedServer runs a job end to end and issues a key on the PostgreSQL store.
func TestPostgresBackedServer(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	cfg, _ := testConfig(t)
	cfg.DatabaseURL = dsn
	cfg.APIKey = "root-admin-key-0123456789"
	srv, deps := newTestServer(t, cfg)
	if !cfg.UsesPostgres() {
		t.Fatal("expected postgres store")
	}

	in := filepath.Join(cfg.InputDirs[0], "in.csv")
	_ = os.WriteFile(in, []byte("team,amount\nA,1\nA,2\nB,x\n"), 0o644)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", strings.NewReader(
		`{"sources":[{"type":"csv","name":"s","path":`+strconv.Quote(in)+`}],"rules":[{"field":"amount","type":"number"}],"aggregations":[{"group_by":"team","field":"amount","operation":"sum"}],"exports":[{"type":"json"}]}`))
	req.Header.Set("X-API-Key", cfg.APIKey)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	var job models.Job
	_ = json.Unmarshal(rec.Body.Bytes(), &job)
	waitForJobCompletion(t, deps.Store, job.ID)

	got, _, _ := deps.Store.GetJob(context.Background(), job.ID)
	if got.Status != models.StatusCompleted || got.Metrics.Processed != 2 || got.Metrics.Rejected != 1 || got.Owner == "" {
		t.Fatalf("unexpected job on postgres: %+v", got)
	}
	if _, total, _ := deps.Store.GetErrors(context.Background(), job.ID, 0, 0); total != 1 {
		t.Fatalf("expected 1 stored error, got %d", total)
	}
	if _, err := deps.Keys.Create(context.Background(), "pg-key", time.Hour); err != nil {
		t.Fatalf("create key on postgres: %v", err)
	}
}
