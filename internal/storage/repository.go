package storage

import (
	"context"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// ListOptions filters and paginates job listings. A Limit <= 0 returns every match.
type ListOptions struct {
	Status models.JobStatus
	Offset int
	Limit  int
}

// Repository specifies the durable persistence contract for pipeline jobs, records, and errors.
//
// Listing methods return the requested page together with the total number of matching rows.
// Jobs are listed newest first; records and errors are returned in insertion order.
type Repository interface {
	SaveJob(ctx context.Context, job *models.Job) error
	GetJob(ctx context.Context, id string) (*models.Job, bool, error)
	ListJobs(ctx context.Context, opts ListOptions) ([]models.Job, int, error)
	DeleteJob(ctx context.Context, id string) error

	AddErrors(ctx context.Context, jobID string, errs []models.RecordError) error
	GetErrors(ctx context.Context, jobID string, offset, limit int) ([]models.RecordError, int, error)

	SaveRecords(ctx context.Context, jobID string, records []models.Record) error
	GetRecords(ctx context.Context, jobID string, offset, limit int) ([]models.Record, int, error)

	Coordinator
	APIKeyStore

	Close() error
}

// Coordinator lets several replicas share one job store safely.
//
// The replica that runs a job heartbeats it; any replica can request cancellation (the owner
// polls for the flag); and any replica can fail jobs whose owner stopped heartbeating.
type Coordinator interface {
	// Heartbeat records that owner is still running the given jobs.
	Heartbeat(ctx context.Context, owner string, jobIDs []string, at time.Time) error
	// RequestCancel flags a PENDING/RUNNING job for cancellation. It reports whether the job
	// exists and whether it was still active.
	RequestCancel(ctx context.Context, id string) (found, active bool, err error)
	// CancelRequested returns the subset of jobIDs that have a pending cancellation request.
	CancelRequested(ctx context.Context, jobIDs []string) ([]string, error)
	// FailStaleJobs marks PENDING/RUNNING jobs whose last heartbeat is before cutoff as FAILED
	// with the given reason, and returns their IDs. A job that heartbeats concurrently is left alone.
	FailStaleJobs(ctx context.Context, cutoff time.Time, reason string) ([]string, error)
}

// APIKeyStore persists managed API keys by the SHA-256 hash of their secret.
type APIKeyStore interface {
	CreateAPIKey(ctx context.Context, key models.APIKey, secretHash string) error
	// FindAPIKeyByHash returns the key whose secret hashes to secretHash, or nil if none.
	FindAPIKeyByHash(ctx context.Context, secretHash string) (*models.APIKey, error)
	ListAPIKeys(ctx context.Context) ([]models.APIKey, error)
	// RevokeAPIKey revokes an active key; it reports whether a key with that ID exists.
	RevokeAPIKey(ctx context.Context, id string, at time.Time) (bool, error)
	TouchAPIKey(ctx context.Context, id string, at time.Time) error
	// DeleteAPIKeysBefore removes keys that expired or were revoked before cutoff.
	DeleteAPIKeysBefore(ctx context.Context, cutoff time.Time) (int, error)
}

// page slices a list using offset/limit semantics shared by all repositories.
func page[T any](list []T, offset, limit int) []T {
	if offset < 0 {
		offset = 0
	}
	if offset >= len(list) {
		return []T{}
	}
	end := len(list)
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}
	out := make([]T, end-offset)
	copy(out, list[offset:end])
	return out
}
