package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/ingestion"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/transformation"
	"github.com/anandmindfire/data-processing-pipeline/internal/validation"
)

type transformResult struct {
	item   ingestion.Item
	record models.Record
}

// startValidationWorkers launches a pool of workers validating records concurrently (fan-out).
// Ownership: the caller closes raw after ingestion finishes and closes valid after the returned
// WaitGroup completes.
func startValidationWorkers(
	ctx context.Context,
	workerCount int,
	validator validation.Validator,
	rules []models.Rule,
	raw <-chan ingestion.Item,
	valid chan<- ingestion.Item,
	errCh chan<- models.RecordError,
	progressCh chan<- progressMsg,
) *sync.WaitGroup {
	var wg sync.WaitGroup
	for n := 0; n < workerCount; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lat := newLatencyAccumulator(progressCh, "validation")
			defer lat.flush()
			for item := range raw {
				if ctx.Err() != nil {
					return
				}
				start := time.Now()
				err := validator.Validate(item.Record, rules)
				lat.add(time.Since(start))

				if err != nil {
					select {
					case errCh <- models.RecordError{
						Source:  item.Source,
						Index:   item.Index,
						Stage:   "validation",
						Message: err.Error(),
						Record:  item.Record,
						At:      time.Now(),
					}:
					case <-ctx.Done():
						return
					}
					continue
				}

				select {
				case valid <- item:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	return &wg
}

// startTransformationWorkers launches a pool of workers applying record transformations concurrently.
func startTransformationWorkers(
	ctx context.Context,
	workerCount int,
	transformer transformation.Transformer,
	rules []models.Rule,
	valid <-chan ingestion.Item,
	transformed chan<- transformResult,
	errCh chan<- models.RecordError,
	progressCh chan<- progressMsg,
) *sync.WaitGroup {
	var wg sync.WaitGroup
	for n := 0; n < workerCount; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lat := newLatencyAccumulator(progressCh, "transform")
			defer lat.flush()
			for item := range valid {
				if ctx.Err() != nil {
					return
				}

				start := time.Now()
				rec, err := transformer.Transform(item.Record, rules)
				lat.add(time.Since(start))
				if err != nil {
					select {
					case errCh <- models.RecordError{
						Source:  item.Source,
						Index:   item.Index,
						Stage:   "transform",
						Message: err.Error(),
						Record:  item.Record,
						At:      time.Now(),
					}:
					case <-ctx.Done():
						return
					}
					continue
				}

				select {
				case transformed <- transformResult{item: item, record: rec}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	return &wg
}
