package storage

import (
	"context"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
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
func (s *Store) PutJob(j *models.Job) error {
	return s.repo.SaveJob(context.Background(), j)
}

// GetJob retrieves a job by ID.
func (s *Store) GetJob(id string) (*models.Job, bool) {
	j, found, err := s.repo.GetJob(context.Background(), id)
	if err != nil || !found {
		return nil, false
	}
	return j, true
}

// ListJobs returns all jobs.
func (s *Store) ListJobs() []models.Job {
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

// AddRecord stores a processed record for a job.
func (s *Store) AddRecord(id string, r models.Record) error {
	return s.repo.SaveRecord(context.Background(), id, r)
}

// Records retrieves records for a job.
func (s *Store) Records(id string) []models.Record {
	records, err := s.repo.GetRecords(context.Background(), id)
	if err != nil {
		return nil
	}
	return records
}

// AddError records an error.
func (s *Store) AddError(id string, e models.RecordError) error {
	return s.repo.AddError(context.Background(), id, e)
}

// Errors retrieves errors for a job.
func (s *Store) Errors(id string) []models.RecordError {
	errs, err := s.repo.GetErrors(context.Background(), id)
	if err != nil {
		return nil
	}
	return errs
}

// Repository returns the underlying storage.Repository.
func (s *Store) Repository() Repository {
	return s.repo
}
