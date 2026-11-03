package storage

import (
	"context"

	"github.com/anandkr47/data-processing-pipeline/internal/domain"
)

// Store is an adapter over FileRepository to preserve backward compatibility.
type Store struct {
	repo *FileRepository
}

// New creates or loads a Store from dir.
func New(dir string) (*Store, error) {
	r, err := NewFileRepository(dir)
	if err != nil {
		return nil, err
	}
	return &Store{repo: r}, nil
}

// PutJob stores a job.
func (s *Store) PutJob(j *domain.Job) error {
	return s.repo.SaveJob(context.Background(), j)
}

// GetJob retrieves a job by ID.
func (s *Store) GetJob(id string) (*domain.Job, bool) {
	j, found, err := s.repo.GetJob(context.Background(), id)
	if err != nil || !found {
		return nil, false
	}
	return j, true
}

// ListJobs returns all jobs.
func (s *Store) ListJobs() []domain.Job {
	jobs, err := s.repo.ListJobs(context.Background())
	if err != nil {
		return nil
	}
	return jobs
}

// DeleteJob deletes a job.
func (s *Store) DeleteJob(id string) error {
	return s.repo.DeleteJob(context.Background(), id)
}

// AddError records an error.
func (s *Store) AddError(id string, e domain.RecordError) error {
	return s.repo.AddError(context.Background(), id, e)
}

// Errors retrieves errors for a job.
func (s *Store) Errors(id string) []domain.RecordError {
	errs, err := s.repo.GetErrors(context.Background(), id)
	if err != nil {
		return nil
	}
	return errs
}

// AddRecord stores a record.
func (s *Store) AddRecord(id string, r domain.Record) error {
	return s.repo.SaveRecord(context.Background(), id, r)
}

// Records returns all records for a job.
func (s *Store) Records(id string) []domain.Record {
	recs, err := s.repo.GetRecords(context.Background(), id)
	if err != nil {
		return nil
	}
	return recs
}

// Repository returns the underlying FileRepository.
func (s *Store) Repository() *FileRepository {
	return s.repo
}
