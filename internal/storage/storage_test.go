package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

func testRepositoryContract(t *testing.T, repo Repository) {
	ctx := context.Background()
	jobID := "contract-test-job"
	now := time.Now().Truncate(time.Millisecond)

	job := &models.Job{
		ID:        jobID,
		Status:    models.StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
		Spec: models.JobSpec{
			Sources: []models.Source{{Type: "csv", Name: "source-1"}},
		},
	}

	t.Run("SaveAndGetJob", func(t *testing.T) { testSaveAndGetJob(t, ctx, repo, job) })
	t.Run("ListJobs", func(t *testing.T) { testListJobs(t, ctx, repo) })
	t.Run("Records", func(t *testing.T) { testSaveAndGetRecords(t, ctx, repo, jobID) })
	t.Run("Errors", func(t *testing.T) { testAddAndGetErrors(t, ctx, repo, jobID, now) })
	t.Run("DeleteJob", func(t *testing.T) { testDeleteJob(t, ctx, repo, jobID) })
	t.Run("Coordination", func(t *testing.T) { testCoordination(t, ctx, repo) })
	t.Run("APIKeys", func(t *testing.T) { testAPIKeys(t, ctx, repo) })
}

func testCoordination(t *testing.T, ctx context.Context, repo Repository) {
	t.Helper()
	now := time.Now()
	mk := func(id, owner string, status models.JobStatus, updated time.Time) {
		if err := repo.SaveJob(ctx, &models.Job{ID: id, Owner: owner, Status: status, CreatedAt: updated, UpdatedAt: updated}); err != nil {
			t.Fatal(err)
		}
	}
	mk("live", "node-a", models.StatusRunning, now.Add(-time.Hour))
	mk("dead", "node-b", models.StatusRunning, now.Add(-time.Hour))
	mk("done", "node-b", models.StatusCompleted, now.Add(-time.Hour))
	defer func() {
		for _, id := range []string{"live", "dead", "done"} {
			_ = repo.DeleteJob(ctx, id)
		}
	}()

	// node-a heartbeats its job; a heartbeat from the wrong owner must be ignored.
	if err := repo.Heartbeat(ctx, "node-a", []string{"live"}, now); err != nil {
		t.Fatal(err)
	}
	if err := repo.Heartbeat(ctx, "node-a", []string{"dead"}, now); err != nil {
		t.Fatal(err)
	}

	found, active, err := repo.RequestCancel(ctx, "live")
	if err != nil || !found || !active {
		t.Fatalf("RequestCancel(live) = %v %v %v", found, active, err)
	}
	if found, active, _ = repo.RequestCancel(ctx, "done"); !found || active {
		t.Fatalf("terminal job must not be flagged: found=%v active=%v", found, active)
	}
	if found, _, _ = repo.RequestCancel(ctx, "missing"); found {
		t.Fatal("missing job reported as found")
	}
	flagged, err := repo.CancelRequested(ctx, []string{"live", "dead"})
	if err != nil || len(flagged) != 1 || flagged[0] != "live" {
		t.Fatalf("CancelRequested = %v %v", flagged, err)
	}
	// A progress snapshot (SaveJob) must not clear a pending cancellation.
	mk("live", "node-a", models.StatusRunning, now)
	if flagged, _ = repo.CancelRequested(ctx, []string{"live"}); len(flagged) != 1 {
		t.Fatal("SaveJob cleared the cancel flag")
	}

	failed, err := repo.FailStaleJobs(ctx, now.Add(-time.Minute), "interrupted: owner stopped")
	if err != nil || len(failed) != 1 || failed[0] != "dead" {
		t.Fatalf("FailStaleJobs = %v %v", failed, err)
	}
	got, _, _ := repo.GetJob(ctx, "dead")
	if got.Status != models.StatusFailed || got.Error != "interrupted: owner stopped" || got.Metrics.EndedAt == nil {
		t.Fatalf("stale job not failed correctly: %+v", got)
	}
	if got, _, _ = repo.GetJob(ctx, "live"); got.Status != models.StatusRunning {
		t.Fatalf("live job must keep running, got %s", got.Status)
	}
}

func testAPIKeys(t *testing.T, ctx context.Context, repo Repository) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	k := models.APIKey{ID: "key_1", Name: "ci", Prefix: "dpp_abcd", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := repo.CreateAPIKey(ctx, k, "hash-1"); err != nil {
		t.Fatal(err)
	}
	old := models.APIKey{ID: "key_old", Name: "old", Prefix: "dpp_old0", CreatedAt: now.Add(-48 * time.Hour), ExpiresAt: now.Add(-24 * time.Hour)}
	if err := repo.CreateAPIKey(ctx, old, "hash-old"); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateAPIKey(ctx, models.APIKey{ID: "key_dup", Name: "d", CreatedAt: now, ExpiresAt: now}, "hash-1"); err == nil {
		t.Fatal("duplicate secret hash must be rejected")
	}

	got, err := repo.FindAPIKeyByHash(ctx, "hash-1")
	if err != nil || got == nil || got.Name != "ci" || !got.ExpiresAt.Equal(k.ExpiresAt) || got.RevokedAt != nil {
		t.Fatalf("FindAPIKeyByHash = %+v %v", got, err)
	}
	if missing, err := repo.FindAPIKeyByHash(ctx, "nope"); err != nil || missing != nil {
		t.Fatalf("expected nil for unknown hash, got %+v %v", missing, err)
	}
	if err := repo.TouchAPIKey(ctx, "key_1", now); err != nil {
		t.Fatal(err)
	}
	if ok, err := repo.RevokeAPIKey(ctx, "key_1", now); !ok || err != nil {
		t.Fatalf("RevokeAPIKey = %v %v", ok, err)
	}
	if ok, _ := repo.RevokeAPIKey(ctx, "missing", now); ok {
		t.Fatal("revoking a missing key must report false")
	}
	got, _ = repo.FindAPIKeyByHash(ctx, "hash-1")
	if got.RevokedAt == nil || got.LastUsedAt == nil || got.Valid(now) {
		t.Fatalf("expected revoked, touched key: %+v", got)
	}
	list, err := repo.ListAPIKeys(ctx)
	if err != nil || len(list) < 2 || list[0].CreatedAt.Before(list[len(list)-1].CreatedAt) {
		t.Fatalf("ListAPIKeys = %+v %v", list, err)
	}
	n, err := repo.DeleteAPIKeysBefore(ctx, now.Add(-time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("DeleteAPIKeysBefore = %d %v", n, err)
	}
	if gone, _ := repo.FindAPIKeyByHash(ctx, "hash-old"); gone != nil {
		t.Fatal("expired key should have been purged")
	}
	_, _ = repo.DeleteAPIKeysBefore(ctx, now.Add(48*time.Hour))
}

// TestPostgresRepository runs the shared contract against PostgreSQL when TEST_POSTGRES_URL is
// set (CI provides a postgres service; locally: TEST_POSTGRES_URL=postgres://... go test ./...).
func TestPostgresRepository(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := context.Background()
	repo, err := NewPostgresRepository(ctx, dsn, PostgresOptions{})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer repo.Close()
	if _, err := repo.db.ExecContext(ctx, `TRUNCATE jobs, records, record_errors, api_keys`); err != nil {
		t.Fatal(err)
	}
	testRepositoryContract(t, repo)

	// A second instance (another replica starting) must migrate idempotently under the lock.
	repo2, err := NewPostgresRepository(ctx, dsn, PostgresOptions{})
	if err != nil {
		t.Fatalf("second replica: %v", err)
	}
	_ = repo2.Close()

	// Large batches exercise the chunked multi-row INSERT.
	job := &models.Job{ID: "pg-bulk", Status: models.StatusRunning, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := repo.SaveJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	recs := make([]models.Record, 1234)
	for i := range recs {
		recs[i] = models.Record{"i": float64(i)}
	}
	if err := repo.SaveRecords(ctx, job.ID, recs); err != nil {
		t.Fatal(err)
	}
	page, total, err := repo.GetRecords(ctx, job.ID, 1230, 0)
	if err != nil || total != 1234 || len(page) != 4 || page[0]["i"] != 1230.0 {
		t.Fatalf("bulk records: total=%d page=%v err=%v", total, page, err)
	}
	_ = repo.DeleteJob(ctx, job.ID)
}

func testSaveAndGetJob(t *testing.T, ctx context.Context, repo Repository, job *models.Job) {
	t.Helper()
	if err := repo.SaveJob(ctx, job); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}
	got, found, err := repo.GetJob(ctx, job.ID)
	if err != nil || !found || got.ID != job.ID {
		t.Fatalf("GetJob failed: found=%v, err=%v, got=%+v", found, err, got)
	}
}

func testListJobs(t *testing.T, ctx context.Context, repo Repository) {
	t.Helper()
	jobs, total, err := repo.ListJobs(ctx, ListOptions{})
	if err != nil || len(jobs) != 1 || total != 1 {
		t.Fatalf("ListJobs failed: count=%d total=%d, err=%v", len(jobs), total, err)
	}

	// Newest first, status filter, and pagination.
	older := &models.Job{ID: "older", Status: models.StatusCompleted, CreatedAt: jobs[0].CreatedAt.Add(-time.Hour), UpdatedAt: time.Now()}
	if err := repo.SaveJob(ctx, older); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = repo.DeleteJob(ctx, older.ID) }()
	jobs, total, _ = repo.ListJobs(ctx, ListOptions{})
	if total != 2 || jobs[0].ID != "contract-test-job" || jobs[1].ID != "older" {
		t.Fatalf("expected newest-first ordering, got total=%d %+v", total, jobs)
	}
	jobs, total, _ = repo.ListJobs(ctx, ListOptions{Status: models.StatusCompleted})
	if total != 1 || jobs[0].ID != "older" {
		t.Fatalf("status filter failed: total=%d %+v", total, jobs)
	}
	jobs, total, _ = repo.ListJobs(ctx, ListOptions{Offset: 1, Limit: 1})
	if total != 2 || len(jobs) != 1 || jobs[0].ID != "older" {
		t.Fatalf("pagination failed: total=%d %+v", total, jobs)
	}
}

func testSaveAndGetRecords(t *testing.T, ctx context.Context, repo Repository, jobID string) {
	t.Helper()
	batch := []models.Record{{"name": "Alice", "score": 95.5}, {"name": "Bob"}, {"name": "Cara"}}
	if err := repo.SaveRecords(ctx, jobID, batch); err != nil {
		t.Fatalf("SaveRecords failed: %v", err)
	}
	records, total, err := repo.GetRecords(ctx, jobID, 0, 0)
	if err != nil || total != 3 || len(records) != 3 || records[0]["name"] != "Alice" || records[0]["score"] != 95.5 {
		t.Fatalf("GetRecords failed: %+v total=%d, err=%v", records, total, err)
	}
	records, total, _ = repo.GetRecords(ctx, jobID, 1, 1)
	if total != 3 || len(records) != 1 || records[0]["name"] != "Bob" {
		t.Fatalf("GetRecords paging failed: %+v total=%d", records, total)
	}
}

func testAddAndGetErrors(t *testing.T, ctx context.Context, repo Repository, jobID string, now time.Time) {
	t.Helper()
	recErr := models.RecordError{
		Source:  "source-1",
		Index:   1,
		Stage:   "validation",
		Message: "invalid record",
		Record:  models.Record{"name": ""},
		At:      now,
	}
	if err := repo.AddErrors(ctx, jobID, []models.RecordError{recErr}); err != nil {
		t.Fatalf("AddErrors failed: %v", err)
	}
	errs, total, err := repo.GetErrors(ctx, jobID, 0, 0)
	if err != nil || total != 1 || len(errs) != 1 || errs[0].Message != "invalid record" || errs[0].Record["name"] != "" {
		t.Fatalf("GetErrors failed: %+v, err=%v", errs, err)
	}
}

func testDeleteJob(t *testing.T, ctx context.Context, repo Repository, jobID string) {
	t.Helper()
	if err := repo.DeleteJob(ctx, jobID); err != nil {
		t.Fatalf("DeleteJob failed: %v", err)
	}
	_, found, err := repo.GetJob(ctx, jobID)
	if err != nil || found {
		t.Fatalf("expected job to be deleted: found=%v, err=%v", found, err)
	}
}

func TestMemoryRepository(t *testing.T) {
	repo := NewMemoryRepository()
	testRepositoryContract(t, repo)
}

func TestSQLiteRepository(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	ctx := context.Background()
	repo, err := NewSQLiteRepository(ctx, dir)
	if err != nil {
		t.Fatalf("failed to create SQLiteRepository: %v", err)
	}
	testRepositoryContract(t, repo)

	// Data must survive a restart.
	job := &models.Job{ID: "durable", Status: models.StatusCompleted, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := repo.SaveJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := repo.SaveRecords(ctx, job.ID, []models.Record{{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	_ = repo.Close()

	repo2, err := NewSQLiteRepository(ctx, dir)
	if err != nil {
		t.Fatalf("failed to reopen SQLiteRepository: %v", err)
	}
	defer repo2.Close()
	jobs, total, err := repo2.ListJobs(ctx, ListOptions{})
	if err != nil || total != 1 || jobs[0].ID != "durable" {
		t.Fatalf("expected persisted job after restart, got total=%d err=%v", total, err)
	}
	recs, n, _ := repo2.GetRecords(ctx, "durable", 0, 0)
	if n != 1 || recs[0]["k"] != "v" {
		t.Fatalf("expected persisted record after restart, got %+v", recs)
	}
}
