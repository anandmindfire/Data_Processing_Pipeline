package services

import "errors"

// Sentinel errors returned by the job services and mapped to HTTP status codes by the handlers.
var (
	ErrCapacityReached = errors.New("pipeline capacity reached; max active jobs running")
	ErrJobNotFound     = errors.New("pipeline job not found")
	ErrJobStillRunning = errors.New("cannot delete an actively running job; cancel it first")
	ErrJobNotRunning   = errors.New("pipeline job is not running")
	ErrShuttingDown    = errors.New("server is shutting down; not accepting new jobs")
)
