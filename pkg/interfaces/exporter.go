package interfaces

import "context"

// Exporter defines the contract for persisting processed records to destinations.
type Exporter interface {
	Export(ctx context.Context, records []map[string]any, targets any) error
}
