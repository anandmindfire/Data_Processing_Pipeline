package export

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	_ "modernc.org/sqlite"
)

// sqliteSink writes records into a real SQLite database file through database/sql.
//
// Columns are added on demand (ALTER TABLE ... ADD COLUMN) as new record keys appear, so records
// can be streamed without knowing the schema up front. SQLite's dynamic typing stores numbers as
// REAL/INTEGER and text as TEXT within the same column.
type sqliteSink struct {
	path    string
	db      *sql.DB
	columns map[string]struct{}
}

// newSQLiteSink initializes a temporary SQLite database with a base records table.
func newSQLiteSink(path string) (*sqliteSink, error) {
	tmp := partPath(path)
	_ = os.Remove(tmp)
	// Rollback journal (not WAL) so the file is self-contained when renamed into place.
	dsn := "file:" + tmp + "?_pragma=journal_mode(DELETE)&_pragma=synchronous(OFF)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: open sqlite export %q: %w", ErrExportFailed, path, err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE records (_row_id INTEGER PRIMARY KEY AUTOINCREMENT)`); err != nil {
		_ = db.Close()
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("%w: create records table: %w", ErrExportFailed, err)
	}
	return &sqliteSink{path: path, db: db, columns: make(map[string]struct{})}, nil
}

// write alters schema for new columns dynamically and inserts records in a transaction.
func (s *sqliteSink) write(records []models.Record) error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmts := make(map[string]*sql.Stmt)
	defer func() {
		for _, st := range stmts {
			_ = st.Close()
		}
	}()

	for _, rec := range records {
		keys := make([]string, 0, len(rec))
		for k := range rec {
			keys = append(keys, k)
			if _, known := s.columns[k]; !known {
				if _, err := tx.ExecContext(ctx, `ALTER TABLE records ADD COLUMN `+quoteIdent(k)); err != nil {
					return fmt.Errorf("add column %q: %w", k, err)
				}
				s.columns[k] = struct{}{}
			}
		}
		if len(keys) == 0 {
			continue
		}
		sort.Strings(keys)
		sig := strings.Join(keys, "\x00")
		stmt, ok := stmts[sig]
		if !ok {
			cols := make([]string, len(keys))
			for i, k := range keys {
				cols[i] = quoteIdent(k)
			}
			q := fmt.Sprintf("INSERT INTO records (%s) VALUES (%s)", strings.Join(cols, ", "),
				strings.TrimSuffix(strings.Repeat("?, ", len(keys)), ", "))
			if stmt, err = tx.PrepareContext(ctx, q); err != nil {
				return err
			}
			stmts[sig] = stmt
		}
		args := make([]any, len(keys))
		for i, k := range keys {
			args[i] = sqlValue(rec[k])
		}
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return fmt.Errorf("insert record: %w", err)
		}
	}
	return tx.Commit()
}

// finish creates the aggregations table, inserts aggregates, closes db, and publishes the file.
func (s *sqliteSink) finish(aggregates map[string]any) error {
	if _, err := s.db.Exec(`CREATE TABLE aggregations (name TEXT PRIMARY KEY, result TEXT NOT NULL)`); err != nil {
		return err
	}
	for _, name := range sortedKeys(aggregates) {
		b, err := json.Marshal(aggregates[name])
		if err != nil {
			return err
		}
		if _, err := s.db.Exec(`INSERT INTO aggregations (name, result) VALUES (?, ?)`, name, string(b)); err != nil {
			return err
		}
	}
	if err := s.db.Close(); err != nil {
		return err
	}
	return os.Rename(partPath(s.path), s.path)
}

// abort closes the database and removes the uncommitted temporary database file.
func (s *sqliteSink) abort() {
	_ = s.db.Close()
	_ = os.Remove(partPath(s.path))
}

// artifacts returns the finalized SQLite database file path.
func (s *sqliteSink) artifacts() []string { return []string{s.path} }

// ---------------------------------------------------------------------------------------------
// Value helpers shared by sinks.

// isNumeric reports whether the value is an integer, float, or numeric JSON number.
func isNumeric(v any) bool {
	switch x := v.(type) {
	case float64, float32, int, int32, int64, uint, uint32, uint64:
		return true
	case json.Number:
		_, err := x.Float64()
		return err == nil
	}
	return false
}

// sqlValue converts a record value to a database/sql argument.
func sqlValue(v any) any {
	switch x := v.(type) {
	case nil, string, float64, float32, int, int32, int64:
		return x
	case bool:
		if x {
			return 1
		}
		return 0
	case json.Number:
		if f, err := x.Float64(); err == nil {
			return f
		}
		return x.String()
	case map[string]any, []any, models.Record:
		b, _ := json.Marshal(x)
		return string(b)
	default:
		return fmt.Sprint(x)
	}
}

// sqlLiteral renders a value as a SQL literal for the SQL dump.
func sqlLiteral(v any) string {
	switch x := sqlValue(v).(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case int:
		return strconv.Itoa(x)
	default:
		return fmt.Sprint(x)
	}
}

// cellString renders a value for a CSV cell; nested values are JSON-encoded.
func cellString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case map[string]any, []any:
		b, _ := json.Marshal(x)
		return string(b)
	default:
		return fmt.Sprint(x)
	}
}

// quoteIdent quotes a SQL identifier, escaping embedded double quotes.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
