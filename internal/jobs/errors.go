package jobs

import "errors"

var (
	// ErrJobNotFound is returned when querying a nonexistent job.
	ErrJobNotFound = errors.New("pipeline job not found")

	// ErrJobNotRunning is returned when attempting to cancel an inactive job.
	ErrJobNotRunning = errors.New("pipeline job is not running")

	// ErrJobStillRunning is returned when attempting to delete an active job.
	ErrJobStillRunning = errors.New("pipeline job is still running; cancel it first")

	// ErrCapacityReached is returned when active job quota is exceeded.
	ErrCapacityReached = errors.New("system capacity reached: maximum concurrent jobs active")
)
