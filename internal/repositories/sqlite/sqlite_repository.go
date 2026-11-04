package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/repositories"
)

// JobRepository implements repositories.JobRepository on SQLite.
type JobRepository struct {
	db *sql.DB
	mu sync.RWMutex
}

var _ repositories.JobRepository = (*JobRepository)(nil)

// NewJobRepository creates a new SQLite JobRepository.
func NewJobRepository(db *sql.DB) *JobRepository {
	return &JobRepository{db: db}
}

// Save inserts or updates a pipeline job.
func (r *JobRepository) Save(ctx context.Context, job *models.Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	cfgJSON, err := json.Marshal(job.Config)
	if err != nil {
		return fmt.Errorf("failed marshaling config: %w", err)
	}
	aggJSON, err := json.Marshal(job.Aggregates)
	if err != nil {
		return fmt.Errorf("failed marshaling aggregates: %w", err)
	}

	query := `
INSERT INTO pipeline_jobs (id, status, config, created_at, updated_at, results_path, aggregates)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    status = excluded.status,
    config = excluded.config,
    updated_at = excluded.updated_at,
    results_path = excluded.results_path,
    aggregates = excluded.aggregates;`

	_, err = r.db.ExecContext(ctx, query,
		job.ID,
		string(job.Status),
		string(cfgJSON),
		job.CreatedAt.UTC(),
		job.UpdatedAt.UTC(),
		job.ResultsPath,
		string(aggJSON),
	)
	return err
}

// FindByID retrieves a pipeline job by its unique ID.
func (r *JobRepository) FindByID(ctx context.Context, id string) (*models.Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	query := `SELECT id, status, config, created_at, updated_at, results_path, aggregates FROM pipeline_jobs WHERE id = ?`
	row := r.db.QueryRowContext(ctx, query, id)

	var (
		job         models.Job
		statusStr   string
		cfgStr      string
		aggStr      sql.NullString
		resultsPath sql.NullString
	)

	err := row.Scan(&job.ID, &statusStr, &cfgStr, &job.CreatedAt, &job.UpdatedAt, &resultsPath, &aggStr)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	job.Status = models.JobStatus(statusStr)
	if resultsPath.Valid {
		job.ResultsPath = resultsPath.String
	}
	if err := json.Unmarshal([]byte(cfgStr), &job.Config); err != nil {
		return nil, fmt.Errorf("failed unmarshaling job config: %w", err)
	}
	if aggStr.Valid && aggStr.String != "" {
		_ = json.Unmarshal([]byte(aggStr.String), &job.Aggregates)
	}

	return &job, nil
}

// FindAll lists all stored pipeline jobs ordered by creation time descending.
func (r *JobRepository) FindAll(ctx context.Context) ([]*models.Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	query := `SELECT id, status, config, created_at, updated_at, results_path, aggregates FROM pipeline_jobs ORDER BY created_at DESC`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var jobs []*models.Job
	for rows.Next() {
		var (
			job         models.Job
			statusStr   string
			cfgStr      string
			aggStr      sql.NullString
			resultsPath sql.NullString
		)
		if err := rows.Scan(&job.ID, &statusStr, &cfgStr, &job.CreatedAt, &job.UpdatedAt, &resultsPath, &aggStr); err != nil {
			return nil, err
		}
		job.Status = models.JobStatus(statusStr)
		if resultsPath.Valid {
			job.ResultsPath = resultsPath.String
		}
		_ = json.Unmarshal([]byte(cfgStr), &job.Config)
		if aggStr.Valid && aggStr.String != "" {
			_ = json.Unmarshal([]byte(aggStr.String), &job.Aggregates)
		}
		jobs = append(jobs, &job)
	}
	return jobs, rows.Err()
}

// Delete removes a pipeline job and associated data.
func (r *JobRepository) Delete(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, err := r.db.ExecContext(ctx, `DELETE FROM pipeline_jobs WHERE id = ?`, id)
	return err
}

// Close closes the underlying database handle.
func (r *JobRepository) Close() error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

// ErrorRepository implements repositories.ErrorRepository on SQLite.
type ErrorRepository struct {
	db *sql.DB
	mu sync.RWMutex
}

var _ repositories.ErrorRepository = (*ErrorRepository)(nil)

// NewErrorRepository creates a new SQLite ErrorRepository.
func NewErrorRepository(db *sql.DB) *ErrorRepository {
	return &ErrorRepository{db: db}
}

// Save inserts a record error for a job.
func (r *ErrorRepository) Save(ctx context.Context, jobID string, recErr models.RecordError) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	payloadJSON, _ := json.Marshal(recErr.Record)
	query := `INSERT INTO record_errors (job_id, source, record_index, stage, message, payload, occurred_at) VALUES (?, ?, ?, ?, ?, ?, ?)`
	_, err := r.db.ExecContext(ctx, query, jobID, recErr.Source, recErr.Index, recErr.Stage, recErr.Message, string(payloadJSON), recErr.At.UTC())
	return err
}

// FindByJobID retrieves all record errors for a job.
func (r *ErrorRepository) FindByJobID(ctx context.Context, jobID string) ([]models.RecordError, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	query := `SELECT source, record_index, stage, message, payload, occurred_at FROM record_errors WHERE job_id = ? ORDER BY id ASC`
	rows, err := r.db.QueryContext(ctx, query, jobID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var errorsList []models.RecordError
	for rows.Next() {
		var (
			re      models.RecordError
			payload sql.NullString
		)
		if err := rows.Scan(&re.Source, &re.Index, &re.Stage, &re.Message, &payload, &re.At); err != nil {
			return nil, err
		}
		if payload.Valid && payload.String != "" {
			_ = json.Unmarshal([]byte(payload.String), &re.Record)
		}
		errorsList = append(errorsList, re)
	}
	return errorsList, rows.Err()
}

// DeleteByJobID removes all recorded errors for a job.
func (r *ErrorRepository) DeleteByJobID(ctx context.Context, jobID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, err := r.db.ExecContext(ctx, `DELETE FROM record_errors WHERE job_id = ?`, jobID)
	return err
}

// MetricsRepository implements repositories.MetricsRepository.
type MetricsRepository struct {
	mu      sync.RWMutex
	metrics map[string]models.Metrics
}

var _ repositories.MetricsRepository = (*MetricsRepository)(nil)

// NewMetricsRepository creates an in-memory MetricsRepository.
func NewMetricsRepository() *MetricsRepository {
	return &MetricsRepository{
		metrics: make(map[string]models.Metrics),
	}
}

// Save stores telemetry metrics for a job.
func (r *MetricsRepository) Save(_ context.Context, jobID string, m models.Metrics) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics[jobID] = m
	return nil
}

// FindByJobID retrieves telemetry metrics for a job.
func (r *MetricsRepository) FindByJobID(_ context.Context, jobID string) (*models.Metrics, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.metrics[jobID]
	if !ok {
		return nil, nil
	}
	return &m, nil
}

// DeleteByJobID deletes metrics for a job.
func (r *MetricsRepository) DeleteByJobID(_ context.Context, jobID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.metrics, jobID)
	return nil
}
