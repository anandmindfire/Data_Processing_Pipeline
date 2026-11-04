package ingestion

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

const (
	maxHTTPRetries   = 3
	initialRetryWait = 50 * time.Millisecond
)

func executeSingleHTTPRequest(ctx context.Context, src models.Source, client *http.Client) (io.ReadCloser, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL, nil)
	if err != nil {
		return nil, false, fmt.Errorf("create request for %q: %w", src.Name, err)
	}
	for k, v := range src.Headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, true, err
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp.Body, false, nil
	}
	_ = resp.Body.Close()

	lastErr := fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
	shouldRetry := resp.StatusCode >= 500
	return nil, shouldRetry, lastErr
}

// fetchHTTP retrieves payload from an HTTP/API endpoint with exponential backoff retry for transient failures.
func fetchHTTP(ctx context.Context, src models.Source, client *http.Client) (io.ReadCloser, error) {
	if src.URL == "" {
		return nil, fmt.Errorf("source %q: URL is required for HTTP/API sources", src.Name)
	}
	if err := ValidatePublicURL(src.URL); err != nil {
		return nil, fmt.Errorf("source %q: %w", src.Name, err)
	}
	if client == nil {
		client = http.DefaultClient
	}

	var lastErr error
	backoff := initialRetryWait

	for attempt := 1; attempt <= maxHTTPRetries; attempt++ {
		body, shouldRetry, err := executeSingleHTTPRequest(ctx, src, client)
		if err == nil {
			return body, nil
		}
		lastErr = err
		if !shouldRetry || attempt == maxHTTPRetries {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
		}
	}

	return nil, fmt.Errorf("%w: source %q after %d attempts: %w", ErrHTTPFetchFailed, src.Name, maxHTTPRetries, lastErr)
}
