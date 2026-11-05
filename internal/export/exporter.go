// Package export streams processed records and final aggregations to file and database sinks.
//
// Every sink writes to a temporary "<path>.part" file and renames it into place only when the
// job finishes successfully, so readers never observe a half-written export.
package export

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// Supported export target types.
const (
	TypeJSON   = "json"   // JSON array of records (+ <name>.aggregations.json sidecar)
	TypeCSV    = "csv"    // RFC 4180 CSV (+ <name>.aggregations.json sidecar)
	TypeSQLite = "sqlite" // SQLite database file with `records` and `aggregations` tables
	TypeSQL    = "sql"    // Portable SQL dump (CREATE TABLE + INSERT statements)
)

// SupportedTypes lists every export target type accepted by the exporter.
var SupportedTypes = []string{TypeJSON, TypeCSV, TypeSQLite, TypeSQL}

// IsSupportedType reports whether t is a known export target type.
func IsSupportedType(t string) bool {
	t = strings.ToLower(strings.TrimSpace(t))
	for _, s := range SupportedTypes {
		if s == t {
			return true
		}
	}
	return false
}

// Exporter opens streaming export sessions for a job.
type Exporter interface {
	Open(jobID string, targets []models.ExportTarget) (Session, error)
}

// Session receives record batches while the pipeline runs and finalizes all sinks at the end.
// A Session is used by a single goroutine (the export stage) and is not safe for concurrent use.
type Session interface {
	// Write appends a batch of records to every sink.
	Write(records []models.Record) error
	// Close writes the aggregations, flushes every sink, and atomically publishes the files.
	Close(aggregates map[string]any) error
	// Abort discards all partial output. It is safe to call after a failed Close.
	Abort()
	// Primary returns the path of the first export target.
	Primary() string
	// Artifacts returns every file path the session publishes (including sidecars).
	Artifacts() []string
}

// sink is a single export destination.
type sink interface {
	write(records []models.Record) error
	finish(aggregates map[string]any) error
	abort()
	artifacts() []string
}

// FileExporter is the concrete Exporter writing to local files.
type FileExporter struct {
	defaultDir string
}

// NewExporter creates a FileExporter. Targets without an explicit path are written to defaultDir.
func NewExporter(defaultDir string) *FileExporter {
	if defaultDir == "" {
		defaultDir = "exports"
	}
	return &FileExporter{defaultDir: defaultDir}
}

// DefaultPath returns the path used for a target that has no explicit path.
func DefaultPath(dir, jobID, targetType string) string {
	ext := strings.ToLower(targetType)
	if ext == TypeSQLite {
		ext = "db"
	}
	return filepath.Join(dir, jobID+"."+ext)
}

// Open creates one sink per target. If any sink fails to open, the ones already opened are aborted.
func (e *FileExporter) Open(jobID string, targets []models.ExportTarget) (Session, error) {
	s := &session{}
	for _, target := range targets {
		if strings.TrimSpace(target.Type) == "" {
			continue
		}
		path := target.Path
		if path == "" {
			path = DefaultPath(e.defaultDir, jobID, target.Type)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			s.Abort()
			return nil, fmt.Errorf("%w: create export directory: %w", ErrExportFailed, err)
		}
		snk, err := newSink(strings.ToLower(strings.TrimSpace(target.Type)), path)
		if err != nil {
			s.Abort()
			return nil, err
		}
		if s.primary == "" {
			s.primary = path
		}
		s.sinks = append(s.sinks, snk)
	}
	return s, nil
}

func newSink(targetType, path string) (sink, error) {
	switch targetType {
	case TypeJSON:
		return newJSONSink(path)
	case TypeCSV:
		return newSpoolSink(path, renderCSV, false)
	case TypeSQL:
		return newSpoolSink(path, renderSQLDump, true)
	case TypeSQLite:
		return newSQLiteSink(path)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedExportTarget, targetType)
	}
}

type session struct {
	sinks   []sink
	primary string
}

func (s *session) Write(records []models.Record) error {
	if len(records) == 0 {
		return nil
	}
	for _, snk := range s.sinks {
		if err := snk.write(records); err != nil {
			return fmt.Errorf("%w: %w", ErrExportFailed, err)
		}
	}
	return nil
}

func (s *session) Close(aggregates map[string]any) error {
	var errs []error
	for _, snk := range s.sinks {
		if err := snk.finish(aggregates); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", ErrExportFailed, errors.Join(errs...))
	}
	return nil
}

func (s *session) Abort() {
	for _, snk := range s.sinks {
		snk.abort()
	}
}

func (s *session) Primary() string { return s.primary }

// Artifacts returns unique artifact paths (JSON and CSV exports of the same job share one
// aggregations sidecar when they use the default naming).
func (s *session) Artifacts() []string {
	var out []string
	seen := make(map[string]bool)
	for _, snk := range s.sinks {
		for _, p := range snk.artifacts() {
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	}
	return out
}

// partPath is the temporary path a sink writes to before publishing.
func partPath(path string) string { return path + ".part" }

// aggregationsSidecarPath returns "<dir>/<name>.aggregations.json" for an export path.
func aggregationsSidecarPath(path string) string {
	return strings.TrimSuffix(path, filepath.Ext(path)) + ".aggregations.json"
}
