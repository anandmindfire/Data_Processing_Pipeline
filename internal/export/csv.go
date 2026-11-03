package export

import (
	"encoding/csv"
	"fmt"
	"io"
	"sort"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// writeCSV serializes records into CSV with sorted, deterministic column headers.
func writeCSV(w io.Writer, records []models.Record) error {
	if len(records) == 0 {
		return nil
	}

	headers := extractHeaders(records[0])
	csvWriter := csv.NewWriter(w)

	if err := csvWriter.Write(headers); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}

	for _, rec := range records {
		row := make([]string, len(headers))
		for i, h := range headers {
			row[i] = fmt.Sprint(rec[h])
		}
		if err := csvWriter.Write(row); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}

	csvWriter.Flush()
	return csvWriter.Error()
}

func extractHeaders(firstRecord models.Record) []string {
	headers := make([]string, 0, len(firstRecord))
	for k := range firstRecord {
		headers = append(headers, k)
	}
	sort.Strings(headers)
	return headers
}

