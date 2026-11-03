package ingestion

import "github.com/anandkr47/data-processing-pipeline/internal/models"

// Item wraps a parsed record with source tracking information.
type Item struct {
	Source string
	Index  int
	Record models.Record
}

