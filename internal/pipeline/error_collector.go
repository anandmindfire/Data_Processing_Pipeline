package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

const errorBatchSize = 256

// startErrorCollector is the single goroutine that receives errors from every stage (fan-in),
// updates counters immediately, and persists errors in batches.
//
// Persistence uses a context detached from job cancellation so errors raised while a job is
// being cancelled or timing out are still recorded.
func startErrorCollector(
	ctx context.Context,
	errCh <-chan models.RecordError,
	jobID string,
	store storage.Repository,
	progressCh chan<- progressMsg,
	flushInterval time.Duration,
) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	storeCtx := context.WithoutCancel(ctx)

	go func() {
		defer wg.Done()
		batch := make([]models.RecordError, 0, errorBatchSize)
		flush := func() {
			if len(batch) == 0 {
				return
			}
			if err := store.AddErrors(storeCtx, jobID, batch); err != nil {
				slog.Error("failed persisting record errors", "job_id", jobID, "count", len(batch), "error", err)
			}
			batch = batch[:0]
		}
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()

		for {
			select {
			case er, ok := <-errCh:
				if !ok {
					flush()
					return
				}
				batch = append(batch, er)
				progressCh <- progressMsg{kind: progressError, count: 1}
				if er.Stage == "validation" || er.Stage == "transform" {
					progressCh <- progressMsg{kind: progressRejected, count: 1}
				}
				if len(batch) >= errorBatchSize {
					flush()
				}
			case <-ticker.C:
				flush()
			}
		}
	}()

	return &wg
}

// exportDeadLetterRecords streams every stored error for a job to a JSON file (written atomically).
func exportDeadLetterRecords(ctx context.Context, path string, jobID string, store storage.Repository) (bool, error) {
	if path == "" {
		return false, nil
	}
	const pageSize = 1000
	first, total, err := store.GetErrors(ctx, jobID, 0, pageSize)
	if err != nil {
		return false, err
	}
	if total == 0 {
		return false, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, err
	}
	tmp := path + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return false, err
	}
	defer func() { _ = os.Remove(tmp) }() // no-op after a successful rename

	write := func(s string) error { _, err := f.WriteString(s); return err }
	if err := write("["); err != nil {
		_ = f.Close()
		return false, err
	}
	n := 0
	for offset, page := 0, first; len(page) > 0; {
		for _, e := range page {
			b, err := json.Marshal(e)
			if err != nil {
				_ = f.Close()
				return false, err
			}
			sep := ",\n  "
			if n == 0 {
				sep = "\n  "
			}
			if err := write(sep + string(b)); err != nil {
				_ = f.Close()
				return false, err
			}
			n++
		}
		offset += len(page)
		if offset >= total {
			break
		}
		if page, _, err = store.GetErrors(ctx, jobID, offset, pageSize); err != nil {
			_ = f.Close()
			return false, err
		}
	}
	if err := write("\n]\n"); err != nil {
		_ = f.Close()
		return false, err
	}
	if err := f.Close(); err != nil {
		return false, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return false, fmt.Errorf("publish dead-letter file: %w", err)
	}
	slog.Info("dead-letter records exported", "job_id", jobID, "count", n, "path", path)
	return true, nil
}
