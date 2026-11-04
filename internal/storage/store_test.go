package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

func TestStorePersistenceAndRetrieval(t *testing.T) {
	dir := t.TempDir()
	s1, err := New(dir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	jobID := "job-123"
	now := time.Now().Truncate(time.Millisecond)
	j := &models.Job{
		ID:        jobID,
		Status:    models.StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
		Config: models.Config{
			Sources: []models.Source{{Type: "csv", Name: "test", Path: "test.csv"}},
			Exports: []models.ExportTarget{{Type: "json", Path: "out.json"}},
		},
		Metrics: models.Metrics{Discovered: 10, Processed: 5},
	}

	if err := s1.PutJob(j); err != nil {
		t.Fatalf("PutJob failed: %v", err)
	}

	if err := s1.AddRecord(jobID, models.Record{"id": "1", "val": 42}); err != nil {
		t.Fatalf("AddRecord failed: %v", err)
	}

	recordErr := models.RecordError{
		Source:  "test",
		Index:   1,
		Stage:   "validation",
		Message: "field missing",
		At:      now,
	}
	if err := s1.AddError(jobID, recordErr); err != nil {
		t.Fatalf("AddError failed: %v", err)
	}

	assertInMemoryStoreState(t, s1, jobID)

	// Reopen store from disk and check recovery
	s2, err := New(dir)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	assertRecoveredStoreState(t, s2, jobID)
	assertStoreDeletion(t, s2, jobID)
}

func assertInMemoryStoreState(t *testing.T, s *Store, jobID string) {
	t.Helper()
	gotJob, ok := s.GetJob(jobID)
	if !ok || gotJob.ID != jobID {
		t.Fatalf("expected job %s, got %#v", jobID, gotJob)
	}
	records := s.Records(jobID)
	if len(records) != 1 || records[0]["id"] != "1" {
		t.Fatalf("unexpected records: %#v", records)
	}
	errs := s.Errors(jobID)
	if len(errs) != 1 || errs[0].Message != "field missing" {
		t.Fatalf("unexpected errors: %#v", errs)
	}
	if len(s.ListJobs()) != 1 {
		t.Fatalf("expected 1 job, got %d", len(s.ListJobs()))
	}
}

func assertRecoveredStoreState(t *testing.T, s *Store, jobID string) {
	t.Helper()
	recoveredJob, ok := s.GetJob(jobID)
	if !ok || recoveredJob.ID != jobID {
		t.Fatalf("expected recovered job %s, got %#v", jobID, recoveredJob)
	}
	if len(s.Records(jobID)) != 1 {
		t.Fatalf("expected 1 recovered record, got %d", len(s.Records(jobID)))
	}
	if len(s.Errors(jobID)) != 1 {
		t.Fatalf("expected 1 recovered error, got %d", len(s.Errors(jobID)))
	}
}

func assertStoreDeletion(t *testing.T, s *Store, jobID string) {
	t.Helper()
	if err := s.DeleteJob(jobID); err != nil {
		t.Fatalf("DeleteJob failed: %v", err)
	}
	if _, ok := s.GetJob(jobID); ok {
		t.Fatal("expected job to be deleted")
	}
	if len(s.Records(jobID)) != 0 {
		t.Fatal("expected records to be deleted with job")
	}
	if len(s.Errors(jobID)) != 0 {
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
