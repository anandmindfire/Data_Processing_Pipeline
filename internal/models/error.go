package models

import "time"

// RecordError encapsulates an error encountered by a specific record in any pipeline stage.
type RecordError struct {
	Source  string    `json:"source"`
	Index   int       `json:"index"`
	Stage   string    `json:"stage"`
	Message string    `json:"message"`
	Record  Record    `json:"record,omitempty"`
	At      time.Time `json:"at"`
}
