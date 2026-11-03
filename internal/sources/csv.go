package sources

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

// readCSV streams rows from a CSV reader and emits models.Record items.
func readCSV(ctx context.Context, sourceName string, r io.Reader, max int, out chan<- Item, emit func(int, models.Record) error) error {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true

	headers, err := cr.Read()
	if err != nil {
		return fmt.Errorf("read csv headers: %w", err)
	}

	for i := 0; max <= 0 || i < max; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		row, err := cr.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("csv row %d: %w", i+2, err)
		}

		rec := models.Record{}
		for n, h := range headers {
			if n < len(row) {
				rec[strings.TrimSpace(h)] = strings.TrimSpace(row[n])
			}
		}

		if err = emit(i+1, rec); err != nil {
			return err
		}
	}
	return nil
}

