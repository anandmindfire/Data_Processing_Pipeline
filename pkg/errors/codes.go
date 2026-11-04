package errors

type ErrorCode string

const (
	CodeInvalidJSON          ErrorCode = "INVALID_JSON"
	CodeInvalidConfiguration ErrorCode = "INVALID_CONFIG"
	CodeJobNotFound          ErrorCode = "JOB_NOT_FOUND"
	CodeJobStillRunning      ErrorCode = "JOB_STILL_RUNNING"
	CodeJobNotRunning        ErrorCode = "JOB_NOT_RUNNING"
	CodeCapacityReached      ErrorCode = "CAPACITY_REACHED"
	CodeInternalError        ErrorCode = "INTERNAL_ERROR"
	CodeRateLimitExceeded    ErrorCode = "RATE_LIMIT_EXCEEDED"
)
