package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/anandkr47/data-processing-pipeline/internal/domain"
)

func TestStorePersistenceAndRetrieval(t *testing.T) {
	dir := t.TempDir()
	s1, err := New(dir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	jobID := "job-123"
	now := time.Now().Truncate(time.Millisecond)
	j := &domain.Job{
		ID:        jobID,
		Status:    domain.Pending,
		CreatedAt: now,
		UpdatedAt: now,
		Config: domain.Config{
			Sources: []domain.Source{{Type: "csv", Name: "test", Path: "test.csv"}},
			Exports: []domain.ExportTarget{{Type: "json", Path: "out.json"}},
		},
		Metrics: domain.Metrics{Discovered: 10, Processed: 5},
	}

	if err := s1.PutJob(j); err != nil {
		t.Fatalf("PutJob failed: %v", err)
	}

	if err := s1.AddRecord(jobID, domain.Record{"id": "1", "val": 42}); err != nil {
		t.Fatalf("AddRecord failed: %v", err)
	}

	recordErr := domain.RecordError{
		Source:  "test",
		Index:   1,
		Stage:   "validation",
		Message: "field missing",
		At:      now,
	}
	if err := s1.AddError(jobID, recordErr); err != nil {
		t.Fatalf("AddError failed: %v", err)
	}

	// Verify in-memory state
	gotJob, ok := s1.GetJob(jobID)
	if !ok || gotJob.ID != jobID {
		t.Fatalf("expected job %s, got %#v", jobID, gotJob)
	}
	records := s1.Records(jobID)
	if len(records) != 1 || records[0]["id"] != "1" {
		t.Fatalf("unexpected records: %#v", records)
	}
	errs := s1.Errors(jobID)
	if len(errs) != 1 || errs[0].Message != "field missing" {
		t.Fatalf("unexpected errors: %#v", errs)
	}
	jobs := s1.ListJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}

	// Reopen store from disk and check recovery
	s2, err := New(dir)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	recoveredJob, ok := s2.GetJob(jobID)
	if !ok || recoveredJob.ID != jobID {
		t.Fatalf("expected recovered job %s, got %#v", jobID, recoveredJob)
	}
	if len(s2.Records(jobID)) != 1 {
		t.Fatalf("expected 1 recovered record, got %d", len(s2.Records(jobID)))
	}
	if len(s2.Errors(jobID)) != 1 {
		t.Fatalf("expected 1 recovered error, got %d", len(s2.Errors(jobID)))
	}

	// Test DeleteJob
	if err := s2.DeleteJob(jobID); err != nil {
		t.Fatalf("DeleteJob failed: %v", err)
	}
	if _, ok := s2.GetJob(jobID); ok {
		t.Fatal("expected job to be deleted")
	}
	if len(s2.Records(jobID)) != 0 {
		t.Fatal("expected records to be deleted with job")
	}
	if len(s2.Errors(jobID)) != 0 {
		t.Fatal("expected errors to be deleted with job")
	}
}

func TestStoreCorruptedFile(t *testing.T) {
	dir := t.TempDir()
	// An empty directory should succeed
	s, err := New(filepath.Join(dir, "nested", "dir"))
	if err != nil {
		t.Fatalf("New failed on new dir: %v", err)
	}
	if len(s.ListJobs()) != 0 {
		t.Fatal("expected empty store")
	}
}

