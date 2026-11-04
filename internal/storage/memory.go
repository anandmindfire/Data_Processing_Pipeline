package storage

import (
	"context"
	"encoding/json"
	"sort"
	"sync"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// MemoryRepository provides a thread-safe in-memory implementation of Repository.
// It is intended for tests and ephemeral runs; data is lost when the process exits.
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

// cloneJob deep-copies a job so callers never share maps or slices with the repository.
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
	c := cloneJob(job)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.ID] = c
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

// ListJobs returns matching jobs, newest first.
func (m *MemoryRepository) ListJobs(_ context.Context, opts ListOptions) ([]models.Job, int, error) {
	m.mu.RLock()
	all := make([]models.Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		if opts.Status != "" && j.Status != opts.Status {
			continue
		}
		all = append(all, *cloneJob(j))
	}
	m.mu.RUnlock()

	sort.Slice(all, func(a, b int) bool {
		if all[a].CreatedAt.Equal(all[b].CreatedAt) {
			return all[a].ID > all[b].ID
		}
		return all[a].CreatedAt.After(all[b].CreatedAt)
	})
	return page(all, opts.Offset, opts.Limit), len(all), nil
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

// AddErrors appends record errors for a job.
func (m *MemoryRepository) AddErrors(_ context.Context, jobID string, errs []models.RecordError) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors[jobID] = append(m.errors[jobID], errs...)
	return nil
}

// GetErrors retrieves a page of errors recorded for a job.
func (m *MemoryRepository) GetErrors(_ context.Context, jobID string, offset, limit int) ([]models.RecordError, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := m.errors[jobID]
	return page(list, offset, limit), len(list), nil
}

// SaveRecords appends processed records for a job.
func (m *MemoryRepository) SaveRecords(_ context.Context, jobID string, records []models.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[jobID] = append(m.records[jobID], records...)
	return nil
}

// GetRecords retrieves a page of processed records for a job.
func (m *MemoryRepository) GetRecords(_ context.Context, jobID string, offset, limit int) ([]models.Record, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := m.records[jobID]
	return page(list, offset, limit), len(list), nil
}

// Close is a no-op for in-memory storage.
func (m *MemoryRepository) Close() error {
	return nil
}
