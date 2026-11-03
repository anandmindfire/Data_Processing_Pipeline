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

	// 1. Save and Get
	if err := repo.SaveJob(ctx, job); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}
	got, found, err := repo.GetJob(ctx, jobID)
	if err != nil || !found || got.ID != jobID {
		t.Fatalf("GetJob failed: found=%v, err=%v, got=%+v", found, err, got)
	}

	// 2. List
	jobs, err := repo.ListJobs(ctx)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("ListJobs failed: count=%d, err=%v", len(jobs), err)
	}

	// 3. Add & Get Record
	rec := models.Record{"name": "Alice", "score": 95.5}
	if err := repo.SaveRecord(ctx, jobID, rec); err != nil {
		t.Fatalf("SaveRecord failed: %v", err)
	}
	records, err := repo.GetRecords(ctx, jobID)
	if err != nil || len(records) != 1 || records[0]["name"] != "Alice" {
		t.Fatalf("GetRecords failed: %+v, err=%v", records, err)
	}

	// 4. Add & Get Error
	recErr := models.RecordError{
		Source:  "source-1",
		Index:   1,
		Stage:   "validation",
		Message: "invalid record",
		At:      now,
	}
	if err := repo.AddError(ctx, jobID, recErr); err != nil {
		t.Fatalf("AddError failed: %v", err)
	}
	errs, err := repo.GetErrors(ctx, jobID)
	if err != nil || len(errs) != 1 || errs[0].Message != "invalid record" {
		t.Fatalf("GetErrors failed: %+v, err=%v", errs, err)
	}

	// 5. Delete
	if err := repo.DeleteJob(ctx, jobID); err != nil {
		t.Fatalf("DeleteJob failed: %v", err)
	}
	_, found, err = repo.GetJob(ctx, jobID)
	if err != nil || found {
		t.Fatalf("expected job to be deleted: found=%v, err=%v", found, err)
	}
}

func TestMemoryRepository(t *testing.T) {
	repo := NewMemoryRepository()
	testRepositoryContract(t, repo)
}

func TestFileRepository(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	repo, err := NewFileRepository(dir)
	if err != nil {
		t.Fatalf("failed to create FileRepository: %v", err)
	}
	testRepositoryContract(t, repo)

	repo2, err := NewFileRepository(dir)
	if err != nil {
		t.Fatalf("failed to reopen FileRepository: %v", err)
	}
	jobs, err := repo2.ListJobs(context.Background())
	if err != nil {
		t.Fatalf("ListJobs failed after restart: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs after deletion, got %d", len(jobs))
	}
}

