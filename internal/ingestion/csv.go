package ingestion

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// readCSV streams rows from a CSV reader and emits records via emit callback.
func readCSV(ctx context.Context, r io.Reader, max int, emit func(int, models.Record) error) error {
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

		record := buildCSVRecord(headers, row)
		if err = emit(i+1, record); err != nil {
			return err
		}
	}
	return nil
}

func buildCSVRecord(headers []string, row []string) models.Record {
	record := make(models.Record, len(headers))
	for n, h := range headers {
		if n < len(row) {
			record[strings.TrimSpace(h)] = strings.TrimSpace(row[n])
		}
	}
	return record
}

