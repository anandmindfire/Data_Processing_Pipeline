package storage

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// MemoryRepository provides a thread-safe in-memory implementation of Repository.
type MemoryRepository struct {
	mu      sync.RWMutex
	jobs    map[string]*models.Job
	errors  map[string][]models.RecordError
	records map[string][]models.Record
}

// NewMemoryRepository constructs a new MemoryRepository.
func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		jobs:    make(map[string]*models.Job),
		errors:  make(map[string][]models.RecordError),
		records: make(map[string][]models.Record),
	}
}

func cloneJob(j *models.Job) *models.Job {
	if j == nil {
		return nil
	}
	b, _ := json.Marshal(j)
	var c models.Job
	_ = json.Unmarshal(b, &c)
	return &c
}

// SaveJob inserts or updates a job.
func (m *MemoryRepository) SaveJob(_ context.Context, job *models.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.ID] = cloneJob(job)
	return nil
}

// GetJob returns a cloned copy of the job if found.
func (m *MemoryRepository) GetJob(_ context.Context, id string) (*models.Job, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	j, ok := m.jobs[id]
	if !ok {
		return nil, false, nil
	}
	return cloneJob(j), true, nil
}

// ListJobs returns all stored jobs.
func (m *MemoryRepository) ListJobs(_ context.Context) ([]models.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]models.Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		out = append(out, *cloneJob(j))
	}
	return out, nil
}

// DeleteJob removes a job, its errors, and its records.
func (m *MemoryRepository) DeleteJob(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.jobs, id)
	delete(m.errors, id)
	delete(m.records, id)
	return nil
}

// AddError stores a record error for a job.
func (m *MemoryRepository) AddError(_ context.Context, jobID string, err models.RecordError) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors[jobID] = append(m.errors[jobID], err)
	return nil
}

// GetErrors retrieves all errors recorded for a job.
func (m *MemoryRepository) GetErrors(_ context.Context, jobID string) ([]models.RecordError, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := m.errors[jobID]
	out := make([]models.RecordError, len(list))
	copy(out, list)
	return out, nil
}

// SaveRecord stores a processed record for a job.
func (m *MemoryRepository) SaveRecord(_ context.Context, jobID string, record models.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[jobID] = append(m.records[jobID], record)
	return nil
}

// GetRecords retrieves all processed records for a job.
func (m *MemoryRepository) GetRecords(_ context.Context, jobID string) ([]models.Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := m.records[jobID]
	out := make([]models.Record, len(list))
	copy(out, list)
	return out, nil
}

// Close is a no-op for in-memory storage.
func (m *MemoryRepository) Close() error {
	return nil
}

