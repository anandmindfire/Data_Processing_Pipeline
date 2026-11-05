package ingestion

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// csvOptions controls header handling and the field separator of a CSV source.
type csvOptions struct {
	hasHeader bool
	columns   []string // explicit column names; required when there is no usable header
	delimiter rune
}

// csvOptionsFor derives the CSV options from a source definition.
func csvOptionsFor(s models.Source) csvOptions {
	o := csvOptions{hasHeader: true, columns: s.Columns, delimiter: ','}
	if s.HasHeader != nil {
		o.hasHeader = *s.HasHeader
	}
	if s.Delimiter != "" {
		o.delimiter = []rune(s.Delimiter)[0]
	}
	return o
}

// readCSV streams rows from a CSV reader and emits records via emit callback.
//
// Column names come from the header row, from opts.columns (which then replace the header), or,
// for headerless files without columns, are generated as col_1, col_2, ...
func readCSV(ctx context.Context, r io.Reader, max int, opts csvOptions, emit func(int, models.Record) error) error {
	cr := csv.NewReader(r)
	cr.Comma = opts.delimiter
	cr.TrimLeadingSpace = true
	// Real-world CSVs often contain stray quotes (e.g. `"Index", Height(Inches)"`) and ragged rows;
	// accept them rather than failing the whole source.
	cr.LazyQuotes = true
	cr.FieldsPerRecord = -1

	headers := opts.columns
	line := 1
	if opts.hasHeader {
		fileHeader, err := cr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read csv headers: %w", err)
		}
		if len(headers) == 0 {
			headers = fileHeader
		}
		line++
	}

	for i := 0; max <= 0 || i < max; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		row, err := cr.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("csv line %d: %w", line+i, err)
		}
		if len(headers) == 0 {
			headers = generatedColumns(len(row))
		}

		record := buildCSVRecord(headers, row)
		if err = emit(i+1, record); err != nil {
			return err
		}
	}
	return nil
}

// generatedColumns names columns col_1..col_n for headerless files without explicit columns.
func generatedColumns(n int) []string {
	cols := make([]string, n)
	for i := range cols {
		cols[i] = fmt.Sprintf("col_%d", i+1)
	}
	return cols
}

// buildCSVRecord maps header names to row values into a models.Record.
func buildCSVRecord(headers []string, row []string) models.Record {
	record := make(models.Record, len(headers))
	for n, h := range headers {
		if n < len(row) {
			record[cleanHeader(h)] = strings.TrimSpace(row[n])
		}
	}
	return record
}

// cleanHeader trims whitespace and stray surrounding quotes from a header cell.
func cleanHeader(h string) string {
	return strings.Trim(strings.TrimSpace(h), `"' `)
}
