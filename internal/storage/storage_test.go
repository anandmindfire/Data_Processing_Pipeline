package storage

import (
	"context"
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
		Config: models.Config{
			Sources: []models.Source{{Type: "csv", Name: "source-1"}},
		},
	}

	t.Run("SaveAndGetJob", func(t *testing.T) { testSaveAndGetJob(t, ctx, repo, job) })
	t.Run("ListJobs", func(t *testing.T) { testListJobs(t, ctx, repo) })
	t.Run("Records", func(t *testing.T) { testSaveAndGetRecords(t, ctx, repo, jobID) })
	t.Run("Errors", func(t *testing.T) { testAddAndGetErrors(t, ctx, repo, jobID, now) })
	t.Run("DeleteJob", func(t *testing.T) { testDeleteJob(t, ctx, repo, jobID) })
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
