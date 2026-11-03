package stages

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

// Export persists records to the requested targets and returns the primary results file path.
func Export(records []models.Record, targets []models.ExportTarget, defaultDir, jobID string) (string, error) {
	primaryPath := ""

	for _, target := range targets {
		if target.Type == "" {
			continue
		}

		path := target.Path
		if path == "" {
			path = filepath.Join(defaultDir, jobID+"."+strings.ToLower(target.Type))
		}

		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return "", fmt.Errorf("create export directory: %w", err)
		}

		f, err := os.Create(path)
		if err != nil {
			return "", fmt.Errorf("create export file %q: %w", path, err)
		}

		switch strings.ToLower(target.Type) {
		case "json":
			enc := json.NewEncoder(f)
			enc.SetIndent("", "  ")
			err = enc.Encode(records)
		case "csv":
			if len(records) > 0 {
				headers := make([]string, 0, len(records[0]))
				for k := range records[0] {
					headers = append(headers, k)
				}
				sort.Strings(headers)

				w := csv.NewWriter(f)
				if err = w.Write(headers); err == nil {
					for _, r := range records {
						row := make([]string, len(headers))
						for i, k := range headers {
							row[i] = fmt.Sprint(r[k])
						}
						if err = w.Write(row); err != nil {
							break
						}
					}
				}
				w.Flush()
				if err == nil {
					err = w.Error()
				}
			}
		default:
			err = fmt.Errorf("unsupported export target type %q", target.Type)
		}

		closeErr := f.Close()
		if err != nil {
			return "", err
		}
		if closeErr != nil {
			return "", closeErr
		}

		if primaryPath == "" {
			primaryPath = path
		}
	}

	return primaryPath, nil
}

