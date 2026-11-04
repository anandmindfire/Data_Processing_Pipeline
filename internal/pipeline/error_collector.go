package pipeline

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

// startErrorCollector listens for record errors, stores them, and feeds the progress tracker.
func startErrorCollector(
	ctx context.Context,
	errCh <-chan models.RecordError,
	jobID string,
	store storage.Repository,
	progressCh chan<- progressMsg,
) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for er := range errCh {
			_ = store.AddError(ctx, jobID, er)
			progressCh <- progressMsg{kind: progressError, count: 1}
			if er.Stage == "validation" || er.Stage == "transform" {
				progressCh <- progressMsg{kind: progressRejected, count: 1}
			}
		}
	}()

	return &wg
}

// exportDeadLetterRecords writes all stored errors for a job to the dead-letter file sink.
func exportDeadLetterRecords(ctx context.Context, path string, jobID string, store storage.Repository) {
	if path == "" {
		return
	}
	errs, err := store.GetErrors(ctx, jobID)
	if err != nil || len(errs) == 0 {
		return
	}
	b, err := json.MarshalIndent(errs, "", "  ")
	if err != nil {
		return
	}
	_ = os.MkdirAll(filepath.Dir(path), 0755)
	_ = os.WriteFile(path, b, 0644)
	slog.Info("dead-letter records exported", "job_id", jobID, "count", len(errs), "path", path)
}
