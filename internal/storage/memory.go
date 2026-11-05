package storage

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// MemoryRepository provides a thread-safe in-memory implementation of Repository.
// It is intended for tests and ephemeral runs; data is lost when the process exits.
type MemoryRepository struct {
	mu      sync.RWMutex
	jobs    map[string]*models.Job
	errors  map[string][]models.RecordError
	records map[string][]models.Record

	heartbeats map[string]time.Time // job ID -> last heartbeat
	cancels    map[string]bool
	keys       map[string]models.APIKey // key ID -> key
	keyHashes  map[string]string        // secret hash -> key ID
}

// NewMemoryRepository constructs a new MemoryRepository.
func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		jobs:       make(map[string]*models.Job),
		errors:     make(map[string][]models.RecordError),
		records:    make(map[string][]models.Record),
		heartbeats: make(map[string]time.Time),
		cancels:    make(map[string]bool),
		keys:       make(map[string]models.APIKey),
		keyHashes:  make(map[string]string),
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
	m.heartbeats[job.ID] = job.UpdatedAt
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
	delete(m.heartbeats, id)
	delete(m.cancels, id)
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

// Heartbeat records that owner is still running the given jobs.
func (m *MemoryRepository) Heartbeat(_ context.Context, owner string, jobIDs []string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range jobIDs {
		if j, ok := m.jobs[id]; ok && j.Owner == owner {
			m.heartbeats[id] = at
		}
	}
	return nil
}

// RequestCancel flags an active job for cancellation.
func (m *MemoryRepository) RequestCancel(_ context.Context, id string) (bool, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return false, false, nil
	}
	if j.Status.IsTerminal() {
		return true, false, nil
	}
	m.cancels[id] = true
	return true, true, nil
}

// CancelRequested returns the jobs among jobIDs that have a cancellation request.
func (m *MemoryRepository) CancelRequested(_ context.Context, jobIDs []string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for _, id := range jobIDs {
		if m.cancels[id] {
			out = append(out, id)
		}
	}
	return out, nil
}

// FailStaleJobs fails active jobs whose heartbeat is older than cutoff.
func (m *MemoryRepository) FailStaleJobs(_ context.Context, cutoff time.Time, reason string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var failed []string
	now := time.Now().UTC()
	for id, j := range m.jobs {
		if j.Status.IsTerminal() || !m.heartbeats[id].Before(cutoff) {
			continue
		}
		failStale(j, reason, now)
		failed = append(failed, id)
	}
	sort.Strings(failed)
	return failed, nil
}

// CreateAPIKey stores a new key and the hash of its secret.
func (m *MemoryRepository) CreateAPIKey(_ context.Context, k models.APIKey, secretHash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, dup := m.keyHashes[secretHash]; dup {
		return errors.New("create api key: duplicate key hash")
	}
	if _, dup := m.keys[k.ID]; dup {
		return errors.New("create api key: duplicate key id")
	}
	m.keys[k.ID] = k
	m.keyHashes[secretHash] = k.ID
	return nil
}

// FindAPIKeyByHash looks a key up by the hash of its secret.
func (m *MemoryRepository) FindAPIKeyByHash(_ context.Context, secretHash string) (*models.APIKey, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	id, ok := m.keyHashes[secretHash]
	if !ok {
		return nil, nil
	}
	k, ok := m.keys[id]
	if !ok {
		return nil, nil
	}
	return &k, nil
}

// ListAPIKeys returns every key, newest first.
func (m *MemoryRepository) ListAPIKeys(_ context.Context) ([]models.APIKey, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]models.APIKey, 0, len(m.keys))
	for _, k := range m.keys {
		out = append(out, k)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].CreatedAt.After(out[b].CreatedAt) })
	return out, nil
}

// RevokeAPIKey revokes a key; it reports whether the key exists.
func (m *MemoryRepository) RevokeAPIKey(_ context.Context, id string, at time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k, ok := m.keys[id]
	if !ok {
		return false, nil
	}
	if k.RevokedAt == nil {
		k.RevokedAt = &at
		m.keys[id] = k
	}
	return true, nil
}

// TouchAPIKey records when a key was last used.
func (m *MemoryRepository) TouchAPIKey(_ context.Context, id string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if k, ok := m.keys[id]; ok {
		k.LastUsedAt = &at
		m.keys[id] = k
	}
	return nil
}

// DeleteAPIKeysBefore purges keys that expired or were revoked before cutoff.
func (m *MemoryRepository) DeleteAPIKeysBefore(_ context.Context, cutoff time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for id, k := range m.keys {
		if k.ExpiresAt.Before(cutoff) || (k.RevokedAt != nil && k.RevokedAt.Before(cutoff)) {
			delete(m.keys, id)
			for h, kid := range m.keyHashes {
				if kid == id {
					delete(m.keyHashes, h)
				}
			}
			n++
		}
	}
	return n, nil
}
