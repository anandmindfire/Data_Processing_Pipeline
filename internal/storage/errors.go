package storage

import "errors"

var (
	// ErrJobNotFound indicates that a requested job ID does not exist in the repository.
	ErrJobNotFound = errors.New("job not found in repository")
)
