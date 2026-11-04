package pipeline

import (
	"github.com/anandmindfire/data-processing-pipeline/internal/ingestion"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// PipelineChannels aggregates the multi-stage channel topology for a running pipeline.
type PipelineChannels struct {
	Raw         chan ingestion.Item
	Valid       chan ingestion.Item
	Transformed chan transformResult
	Result      chan models.Record
	Errors      chan models.RecordError
	Progress    chan progressMsg
}

// NewPipelineChannels allocates all channel buffers according to the configured bufferSize.
func NewPipelineChannels(bufferSize int) *PipelineChannels {
	if bufferSize <= 0 {
		bufferSize = 64
	}
	return &PipelineChannels{
		Raw:         make(chan ingestion.Item, bufferSize),
		Valid:       make(chan ingestion.Item, bufferSize),
		Transformed: make(chan transformResult, bufferSize),
		Result:      make(chan models.Record, bufferSize),
		Errors:      make(chan models.RecordError, bufferSize),
		Progress:    make(chan progressMsg, bufferSize*2),
	}
}
