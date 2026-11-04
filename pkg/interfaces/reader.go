package interfaces

import "context"

// Reader defines the contract for reading records from a data source.
type Reader interface {
	Read(ctx context.Context, source any, maxRecords int) error
}
