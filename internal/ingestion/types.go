package ingestion

import "github.com/anandmindfire/data-processing-pipeline/internal/models"

// Item wraps a parsed record with source tracking information.
type Item struct {
	Source string
	Index  int
	Record models.Record
}

