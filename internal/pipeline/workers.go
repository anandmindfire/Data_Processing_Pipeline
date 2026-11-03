package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
	"github.com/anandkr47/data-processing-pipeline/internal/pipeline/stages"
	"github.com/anandkr47/data-processing-pipeline/internal/sources"
)

type transformResult struct {
	item   sources.Item
	record models.Record
}

// startValidationWorkers spawns workerCount validation goroutines.
func startValidationWorkers(
	ctx context.Context,
	workerCount int,
	rules []models.Rule,
	raw <-chan sources.Item,
	valid chan<- sources.Item,
	errCh chan<- models.RecordError,
	recordLatency func(string, time.Duration),
) *sync.WaitGroup {
	var wg sync.WaitGroup
	for n := 0; n < workerCount; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range raw {
				start := time.Now()
				if ctx.Err() != nil {
					return
				}
				if err := stages.ValidateRecord(item.Record, rules); err != nil {
					recordLatency("validation", time.Since(start))
					errCh <- models.RecordError{
						Source:  item.Source,
						Index:   item.Index,
						Stage:   "validation",
						Message: err.Error(),
						Record:  item.Record,
						At:      time.Now(),
					}
					continue
				}
				recordLatency("validation", time.Since(start))
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

// startTransformWorkers spawns workerCount transformation goroutines.
func startTransformWorkers(
	ctx context.Context,
	workerCount int,
	rules []models.Rule,
	valid <-chan sources.Item,
	transformed chan<- transformResult,
	errCh chan<- models.RecordError,
	recordLatency func(string, time.Duration),
) *sync.WaitGroup {
	var wg sync.WaitGroup
	for n := 0; n < workerCount; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range valid {
				if ctx.Err() != nil {
					return
				}
				start := time.Now()
				r, err := stages.TransformRecord(item.Record, rules)
				recordLatency("transform", time.Since(start))
				if err != nil {
					errCh <- models.RecordError{
						Source:  item.Source,
						Index:   item.Index,
						Stage:   "transform",
						Message: err.Error(),
						Record:  item.Record,
						At:      time.Now(),
					}
					continue
				}
				select {
				case transformed <- transformResult{item: item, record: r}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	return &wg
}

