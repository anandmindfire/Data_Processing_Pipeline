package mocks

import (
	"context"

	"github.com/anandmindfire/data-processing-pipeline/internal/ingestion"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// MockReader implements ingestion.Reader for testing purposes.
type MockReader struct {
	ItemsToEmit []ingestion.Item
	ErrToReturn error
}

func (m *MockReader) Read(ctx context.Context, source models.Source, maxRecords int, out chan<- ingestion.Item, onDiscovered func()) error {
	if m.ErrToReturn != nil {
		return m.ErrToReturn
	}
	for i, item := range m.ItemsToEmit {
		if maxRecords > 0 && i >= maxRecords {
			break
		}
		if onDiscovered != nil {
			onDiscovered()
		}
		select {
		case out <- item:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
