package pipeline

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	if _, total, err := store.GetErrors(ctx, jobID, 0, 1); err != nil || total == 0 {
		return false, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return false, err
	}
	tmp := path + ".part"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640) //nolint:gosec // path was sandboxed to EXPORT_DIR at submission
	if err != nil {
		return false, err
	}
	defer func() { _ = os.Remove(tmp) }() // no-op after a successful rename

	n, writeErr := writeErrorsJSON(ctx, f, jobID, store)
	if closeErr := f.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return false, writeErr
	}
	if err := os.Rename(tmp, path); err != nil {
		return false, fmt.Errorf("publish dead-letter file: %w", err)
	}
	slog.Info("dead-letter records exported", "job_id", jobID, "count", n, "path", path)
	return true, nil
}

// writeErrorsJSON streams every stored error of a job as a JSON array, one page at a time.
func writeErrorsJSON(ctx context.Context, w io.Writer, jobID string, store storage.Repository) (int, error) {
	const pageSize = 1000
	bw := bufio.NewWriter(w)
	if _, err := bw.WriteString("["); err != nil {
		return 0, err
	}
	n := 0
	for offset := 0; ; offset += pageSize {
		page, total, err := store.GetErrors(ctx, jobID, offset, pageSize)
		if err != nil {
			return n, err
		}
		for _, e := range page {
			b, err := json.Marshal(e)
			if err != nil {
				return n, err
			}
			sep := ",\n  "
			if n == 0 {
				sep = "\n  "
			}
			if _, err := bw.WriteString(sep + string(b)); err != nil {
				return n, err
			}
			n++
		}
		if len(page) == 0 || offset+len(page) >= total {
			break
		}
	}
	if _, err := bw.WriteString("\n]\n"); err != nil {
		return n, err
	}
	return n, bw.Flush()
}
