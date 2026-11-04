package request

// CancelPipelineRequest encapsulates parameters for cancelling an active pipeline job.
type CancelPipelineRequest struct {
	Reason string `json:"reason,omitempty"`
}
