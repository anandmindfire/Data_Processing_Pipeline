package export

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// writeAggregationsSidecar writes aggregations next to a file export (skipped when empty).
func writeAggregationsSidecar(path string, aggregates map[string]any) (string, error) {
	if len(aggregates) == 0 {
		return "", nil
	}
	sidecar := aggregationsSidecarPath(path)
	b, err := json.MarshalIndent(aggregates, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode aggregations: %w", err)
	}
	tmp := partPath(sidecar)
	if err := os.WriteFile(tmp, append(b, '\n'), 0o640); err != nil { //nolint:gosec // exports are meant to be group-readable by downstream consumers
		return "", err
	}
	return sidecar, os.Rename(tmp, sidecar)
}

// ---------------------------------------------------------------------------------------------
// JSON: streamed directly as an array, one record per line.

// jsonSink streams records into a formatted JSON array file on disk.
// Aggregations are written to a sidecar JSON file upon completion.
type jsonSink struct {
	path    string
	file    *os.File
	buf     *bufio.Writer
	count   int
	sidecar string
}

// newJSONSink creates a temporary .part JSON array file and writes the opening bracket.
func newJSONSink(path string) (*jsonSink, error) {
	f, err := os.Create(partPath(path))
	if err != nil {
		return nil, fmt.Errorf("%w: create %q: %w", ErrExportFailed, path, err)
	}
	s := &jsonSink{path: path, file: f, buf: bufio.NewWriterSize(f, 64<<10)}
	_, err = s.buf.WriteString("[")
	return s, err
}

// write formats and streams a batch of records into the active JSON array.
func (s *jsonSink) write(records []models.Record) error {
	for _, rec := range records {
		b, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("encode json record: %w", err)
		}
		sep := ",\n  "
		if s.count == 0 {
			sep = "\n  "
		}
		if _, err := s.buf.WriteString(sep); err != nil {
			return err
		}
		if _, err := s.buf.Write(b); err != nil {
			return err
		}
		s.count++
	}
	return nil
}

// finish closes the JSON array, atomically renames the file, and writes any sidecars.
func (s *jsonSink) finish(aggregates map[string]any) error {
	tail := "\n]\n"
	if s.count == 0 {
		tail = "]\n"
	}
	if _, err := s.buf.WriteString(tail); err != nil {
		return err
	}
	if err := s.buf.Flush(); err != nil {
		return err
	}
	if err := s.file.Close(); err != nil {
		return err
	}
	if err := os.Rename(partPath(s.path), s.path); err != nil {
		return err
	}
	sidecar, err := writeAggregationsSidecar(s.path, aggregates)
	s.sidecar = sidecar
	return err
}

// abort closes and deletes the temporary .part file without publishing.
func (s *jsonSink) abort() {
	_ = s.file.Close()
	_ = os.Remove(partPath(s.path))
}

// artifacts returns the finalized file path and sidecar path if present.
func (s *jsonSink) artifacts() []string { return withSidecar(s.path, s.sidecar) }

// withSidecar bundles the primary export file path with its sidecar path when present.
func withSidecar(path, sidecar string) []string {
	if sidecar == "" {
		return []string{path}
	}
	return []string{path, sidecar}
}

// ---------------------------------------------------------------------------------------------
// Spooled sinks (CSV, SQL dump): the column set is only known after the last record, so records
// are spooled to a JSONL file on disk (bounded memory) and rendered in a single pass at the end.

type columnStats struct {
	nonNumeric bool
}

type renderFunc func(w io.Writer, headers []string, stats map[string]*columnStats, next func() (models.Record, error), aggregates map[string]any) error

// spoolSink buffers records into a temporary file to detect headers before final rendering.
type spoolSink struct {
	path    string
	spool   *os.File
	buf     *bufio.Writer
	stats   map[string]*columnStats
	render  renderFunc
	sidecar string
	withAgg bool
}

// newSpoolSink creates a spooled sink. When embedsAggregates is false the aggregations are
// written to a JSON sidecar next to the export.
func newSpoolSink(path string, render renderFunc, embedsAggregates bool) (*spoolSink, error) {
	f, err := os.Create(path + ".spool")
	if err != nil {
		return nil, fmt.Errorf("%w: create spool for %q: %w", ErrExportFailed, path, err)
	}
	return &spoolSink{
		path:    path,
		spool:   f,
		buf:     bufio.NewWriterSize(f, 64<<10),
		stats:   make(map[string]*columnStats),
		render:  render,
		withAgg: !embedsAggregates,
	}, nil
}

// write tracks dynamic column types and spools raw JSON lines to the temporary disk buffer.
func (s *spoolSink) write(records []models.Record) error {
	for _, rec := range records {
		for k, v := range rec {
			st, ok := s.stats[k]
			if !ok {
				st = &columnStats{}
				s.stats[k] = st
			}
			if v != nil && !isNumeric(v) {
				st.nonNumeric = true
			}
		}
		b, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("encode spool record: %w", err)
		}
		if _, err := s.buf.Write(append(b, '\n')); err != nil {
			return err
		}
	}
	return nil
}

// finish flushes the spool, renders final output via renderFunc, and cleans up temporary files.
func (s *spoolSink) finish(aggregates map[string]any) error {
	if err := s.buf.Flush(); err != nil {
		return err
	}
	if _, err := s.spool.Seek(0, io.SeekStart); err != nil {
		return err
	}

	headers := make([]string, 0, len(s.stats))
	for k := range s.stats {
		headers = append(headers, k)
	}
	sort.Strings(headers)

	out, err := os.Create(partPath(s.path))
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(out, 64<<10)
	reader := bufio.NewReaderSize(s.spool, 64<<10)
	next := func() (models.Record, error) {
		line, err := reader.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) == 0 {
			if err == nil {
				err = io.EOF
			}
			return nil, err
		}
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber() // preserve the original numeric text
		var rec models.Record
		if derr := dec.Decode(&rec); derr != nil {
			return nil, derr
		}
		return rec, nil
	}

	renderErr := s.render(w, headers, s.stats, next, aggregates)
	flushErr := w.Flush()
	closeErr := out.Close()
	if err := errors.Join(renderErr, flushErr, closeErr); err != nil {
		return err
	}
	s.removeSpool()
	if err := os.Rename(partPath(s.path), s.path); err != nil {
		return err
	}
	if s.withAgg {
		sidecar, err := writeAggregationsSidecar(s.path, aggregates)
		s.sidecar = sidecar
		return err
	}
	return nil
}

// removeSpool closes and removes the intermediate disk spool file.
func (s *spoolSink) removeSpool() {
	_ = s.spool.Close()
	_ = os.Remove(s.spool.Name())
}

// abort removes both the intermediate disk spool and the temporary .part output file.
func (s *spoolSink) abort() {
	s.removeSpool()
	_ = os.Remove(partPath(s.path))
}

// artifacts returns the rendered file path and sidecar path if generated.
func (s *spoolSink) artifacts() []string { return withSidecar(s.path, s.sidecar) }

// renderCSV reads spooled records and streams RFC 4180 CSV rows with unified headers.
func renderCSV(w io.Writer, headers []string, _ map[string]*columnStats, next func() (models.Record, error), _ map[string]any) error {
	if len(headers) == 0 {
		return nil
	}
	cw := csv.NewWriter(w)
	if err := cw.Write(headers); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}
	row := make([]string, len(headers))
	for {
		rec, err := next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		for i, h := range headers {
			row[i] = cellString(rec[h])
		}
		if err := cw.Write(row); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}
	cw.Flush()
	return cw.Error()
}

// renderSQLDump generates SQLite-compatible DDL tables, record inserts, and aggregation tables.
func renderSQLDump(w io.Writer, headers []string, stats map[string]*columnStats, next func() (models.Record, error), aggregates map[string]any) error {
	bw := bufio.NewWriter(w)
	fmt.Fprintln(bw, "-- SQL dump generated by data-processing-pipeline (SQLite dialect)")
	fmt.Fprintln(bw, "BEGIN TRANSACTION;")
	fmt.Fprintln(bw, "CREATE TABLE IF NOT EXISTS records (\n    _row_id INTEGER PRIMARY KEY AUTOINCREMENT"+columnDefs(headers, stats)+"\n);")

	cols := make([]string, len(headers))
	for i, h := range headers {
		cols[i] = quoteIdent(h)
	}
	colList := strings.Join(cols, ", ")
	vals := make([]string, len(headers))
	for {
		rec, err := next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		for i, h := range headers {
			vals[i] = sqlLiteral(rec[h])
		}
		fmt.Fprintf(bw, "INSERT INTO records (%s) VALUES (%s);\n", colList, strings.Join(vals, ", "))
	}

	fmt.Fprintln(bw, "CREATE TABLE IF NOT EXISTS aggregations (\n    name TEXT PRIMARY KEY,\n    result TEXT NOT NULL\n);")
	for _, name := range sortedKeys(aggregates) {
		b, err := json.Marshal(aggregates[name])
		if err != nil {
			return err
		}
		fmt.Fprintf(bw, "INSERT INTO aggregations (name, result) VALUES (%s, %s);\n", sqlLiteral(name), sqlLiteral(string(b)))
	}
	fmt.Fprintln(bw, "COMMIT;")
	return bw.Flush()
}

// columnDefs generates typed SQL column definitions based on analyzed record statistics.
func columnDefs(headers []string, stats map[string]*columnStats) string {
	var sb strings.Builder
	for _, h := range headers {
		typ := "NUMERIC"
		if st := stats[h]; st == nil || st.nonNumeric {
			typ = "TEXT"
		}
		sb.WriteString(",\n    ")
		sb.WriteString(quoteIdent(h))
		sb.WriteByte(' ')
		sb.WriteString(typ)
	}
	return sb.String()
}

// sortedKeys returns map keys sorted alphabetically for deterministic output.
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
