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

	headers := extractHeaders(records)
	csvWriter := csv.NewWriter(w)

	if err := csvWriter.Write(headers); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}

	for _, rec := range records {
		row := make([]string, len(headers))
		for i, h := range headers {
			val := rec[h]
			if val == nil {
				row[i] = ""
			} else {
				row[i] = fmt.Sprint(val)
			}
		}
		if err := csvWriter.Write(row); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}

	csvWriter.Flush()
	return csvWriter.Error()
}

func extractHeaders(records []models.Record) []string {
	keySet := make(map[string]struct{})
	for _, r := range records {
		for k := range r {
			keySet[k] = struct{}{}
		}
	}
	headers := make([]string, 0, len(keySet))
	for k := range keySet {
		headers = append(headers, k)
	}
	sort.Strings(headers)
	return headers
}
