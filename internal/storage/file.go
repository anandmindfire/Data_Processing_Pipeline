package storage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

// FileRepository provides atomic JSON snapshot persistence on local disk.
type FileRepository struct {
	mu      sync.RWMutex
	dir     string
	jobs    map[string]*models.Job
	errors  map[string][]models.RecordError
	records map[string][]models.Record
}

// NewFileRepository initializes a FileRepository in the specified directory.
func NewFileRepository(dir string) (*FileRepository, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	repo := &FileRepository{
		dir:     dir,
		jobs:    make(map[string]*models.Job),
		errors:  make(map[string][]models.RecordError),
		records: make(map[string][]models.Record),
	}
	_ = repo.load()
	return repo, nil
}

func (f *FileRepository) load() error {
	b, err := os.ReadFile(filepath.Join(f.dir, "state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var v struct {
		Jobs    map[string]*models.Job          `json:"jobs"`
		Errors  map[string][]models.RecordError `json:"errors"`
		Records map[string][]models.Record      `json:"records"`
	}
	if err = json.Unmarshal(b, &v); err != nil {
		return err
	}
	if v.Jobs != nil {
		f.jobs = v.Jobs
	}
	if v.Errors != nil {
		f.errors = v.Errors
	}
	if v.Records != nil {
		f.records = v.Records
	}
	return nil
}

func (f *FileRepository) persistLocked() error {
	b, err := json.MarshalIndent(struct {
		Jobs    map[string]*models.Job          `json:"jobs"`
		Errors  map[string][]models.RecordError `json:"errors"`
		Records map[string][]models.Record      `json:"records"`
	}{f.jobs, f.errors, f.records}, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(f.dir, "state.tmp")
	if err = os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(f.dir, "state.json"))
}

// SaveJob persists the given job.
func (f *FileRepository) SaveJob(_ context.Context, job *models.Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobs[job.ID] = cloneJob(job)
	return f.persistLocked()
}

// GetJob loads a job by ID.
func (f *FileRepository) GetJob(_ context.Context, id string) (*models.Job, bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	j, ok := f.jobs[id]
	if !ok {
		return nil, false, nil
	}
	return cloneJob(j), true, nil
}

// ListJobs returns all persisted jobs.
func (f *FileRepository) ListJobs(_ context.Context) ([]models.Job, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]models.Job, 0, len(f.jobs))
	for _, j := range f.jobs {
		out = append(out, *cloneJob(j))
	}
	return out, nil
}

// DeleteJob removes a job, its errors, and its records from disk.
func (f *FileRepository) DeleteJob(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.jobs, id)
	delete(f.errors, id)
	delete(f.records, id)
	return f.persistLocked()
}

// AddError records a failure for a job.
func (f *FileRepository) AddError(_ context.Context, jobID string, err models.RecordError) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errors[jobID] = append(f.errors[jobID], err)
	return f.persistLocked()
}

// GetErrors returns all errors recorded for a job.
func (f *FileRepository) GetErrors(_ context.Context, jobID string) ([]models.RecordError, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	list := f.errors[jobID]
	out := make([]models.RecordError, len(list))
	copy(out, list)
	return out, nil
}

// SaveRecord appends a processed record to disk.
func (f *FileRepository) SaveRecord(_ context.Context, jobID string, record models.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records[jobID] = append(f.records[jobID], record)
	return f.persistLocked()
}

// GetRecords retrieves all processed records for a job.
func (f *FileRepository) GetRecords(_ context.Context, jobID string) ([]models.Record, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	list := f.records[jobID]
	out := make([]models.Record, len(list))
	copy(out, list)
	return out, nil
}

// Close flushes and closes file operations.
func (f *FileRepository) Close() error {
	return nil
}

