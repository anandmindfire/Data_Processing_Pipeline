package ingestion

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

// fetchHTTP retrieves payload from an HTTP/API endpoint using context and headers.
func fetchHTTP(ctx context.Context, src models.Source, client *http.Client) (io.ReadCloser, error) {
	if src.URL == "" {
		return nil, fmt.Errorf("source %q: URL is required for HTTP/API sources", src.Name)
	}
	if client == nil {
		client = http.DefaultClient
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request for %q: %w", src.Name, err)
	}

	for k, v := range src.Headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch %q: %w", src.Name, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("source %q: unexpected HTTP status %d", src.Name, resp.StatusCode)
	}

	return resp.Body, nil
}

