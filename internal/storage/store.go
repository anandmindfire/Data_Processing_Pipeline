package storage

import (
	"encoding/json"
	"errors"
	"github.com/anandkr47/data-processing-pipeline/internal/domain"
	"os"
	"path/filepath"
	"sync"
)

type Store struct {
	mu      sync.RWMutex
	dir     string
	jobs    map[string]*domain.Job
	errs    map[string][]domain.RecordError
	records map[string][]domain.Record
}

func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, jobs: map[string]*domain.Job{}, errs: map[string][]domain.RecordError{}, records: map[string][]domain.Record{}}
	_ = s.load()
	return s, nil
}
func (s *Store) load() error {
	b, e := os.ReadFile(filepath.Join(s.dir, "state.json"))
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	var v struct {
		Jobs    map[string]*domain.Job          `json:"jobs"`
		Errors  map[string][]domain.RecordError `json:"errors"`
		Records map[string][]domain.Record      `json:"records"`
	}
	if e = json.Unmarshal(b, &v); e != nil {
		return e
	}
	if v.Jobs != nil {
		s.jobs = v.Jobs
	}
	if v.Errors != nil {
		s.errs = v.Errors
	}
	if v.Records != nil {
		s.records = v.Records
	}
	return nil
}
func (s *Store) persistLocked() error {
	b, e := json.MarshalIndent(struct {
		Jobs    map[string]*domain.Job          `json:"jobs"`
		Errors  map[string][]domain.RecordError `json:"errors"`
		Records map[string][]domain.Record      `json:"records"`
	}{s.jobs, s.errs, s.records}, "", "  ")
	if e != nil {
		return e
	}
	tmp := filepath.Join(s.dir, "state.tmp")
	if e = os.WriteFile(tmp, b, 0644); e != nil {
		return e
	}
	return os.Rename(tmp, filepath.Join(s.dir, "state.json"))
}
func cloneJob(j *domain.Job) *domain.Job {
	b, _ := json.Marshal(j)
	var c domain.Job
	_ = json.Unmarshal(b, &c)
	return &c
}
func (s *Store) PutJob(j *domain.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[j.ID] = cloneJob(j)
	return s.persistLocked()
}
func (s *Store) GetJob(id string) (*domain.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, false
	}
	c := cloneJob(j)
	return c, true
}
func (s *Store) ListJobs() []domain.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, *cloneJob(j))
	}
	return out
}
func (s *Store) DeleteJob(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.jobs, id)
	delete(s.errs, id)
	delete(s.records, id)
	return s.persistLocked()
}
func (s *Store) AddError(id string, e domain.RecordError) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs[id] = append(s.errs[id], e)
	return s.persistLocked()
}
func (s *Store) Errors(id string) []domain.RecordError {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]domain.RecordError(nil), s.errs[id]...)
}
func (s *Store) AddRecord(id string, r domain.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[id] = append(s.records[id], r)
	return s.persistLocked()
}
func (s *Store) Records(id string) []domain.Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]domain.Record(nil), s.records[id]...)
}
