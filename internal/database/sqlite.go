package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
)

// SQLiteConfig holds parameters specific to an embedded SQLite database.
type SQLiteConfig struct {
	FilePath      string
	BusyTimeoutMs int
}

// EnsureSQLiteDir ensures that the target directory for the SQLite file exists.
func EnsureSQLiteDir(filePath string) error {
	dir := filepath.Dir(filePath)
	if dir != "" && dir != "." {
		return os.MkdirAll(dir, 0o750)
	}
	return nil
}

// ApplySQLitePragmas configures performance and integrity pragmas for SQLite.
func ApplySQLitePragmas(ctx context.Context, db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode = WAL;",
		"PRAGMA synchronous = NORMAL;",
		"PRAGMA foreign_keys = ON;",
		"PRAGMA busy_timeout = 5000;",
	}

	for _, p := range pragmas {
		if _, err := db.ExecContext(ctx, p); err != nil {
			return fmt.Errorf("failed executing sqlite pragma %q: %w", p, err)
		}
	}
	return nil
}
