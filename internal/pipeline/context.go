package pipeline

import (
	"context"
	"time"
)

// CreateJobContext creates a derived timeout context for a pipeline job.
func CreateJobContext(parent context.Context, timeoutSeconds int) (context.Context, context.CancelFunc) {
	if timeoutSeconds <= 0 {
		timeoutSeconds = 300
	}
	return context.WithTimeout(parent, time.Duration(timeoutSeconds)*time.Second)
}

// IsCancelled returns true if the context was cancelled or reached its deadline.
func IsCancelled(ctx context.Context) bool {
	return ctx.Err() != nil
}
