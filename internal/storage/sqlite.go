package storage

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	// Registers the pure-Go (CGO-free) "sqlite" database/sql driver.
	_ "modernc.org/sqlite"
)

// SQLiteDriver is the registered driver name for modernc.org/sqlite.
const SQLiteDriver = "sqlite"

// SQLiteDBFile is the database filename created inside the data directory.
const SQLiteDBFile = "pipeline.db"

// NewSQLiteRepository opens (or creates) <dataDir>/pipeline.db and applies schema migrations.
//
// SQLite suits a single instance: the file must not be shared by several replicas. Use
// NewPostgresRepository for multi-replica deployments.
func NewSQLiteRepository(ctx context.Context, dataDir string) (*SQLRepository, error) {
	db, err := openSQLite(ctx, filepath.Join(dataDir, SQLiteDBFile), 4)
	if err != nil {
		return nil, err
	}
	r := &SQLRepository{db: db, d: sqliteDialect}
	if err := r.migrate(ctx, db, sqliteMigrations); err != nil {
		_ = db.Close()
		return nil, err
	}
	return r, nil
}

// sqliteDSN builds a DSN that applies the pragmas to every pooled connection.
func sqliteDSN(filePath string) string {
	q := url.Values{}
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "busy_timeout(5000)")
	q.Set("_txlock", "immediate")
	return "file:" + filePath + "?" + q.Encode()
}

// openSQLite ensures the parent directory exists, connects to SQLite, and verifies connectivity.
func openSQLite(ctx context.Context, filePath string, maxOpenConns int) (*sql.DB, error) {
	if dir := filepath.Dir(filePath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("create sqlite directory: %w", err)
		}
	}
	db, err := sql.Open(SQLiteDriver, sqliteDSN(filePath))
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}
	if maxOpenConns > 0 {
		db.SetMaxOpenConns(maxOpenConns)
		db.SetMaxIdleConns(maxOpenConns)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite database: %w", err)
	}
	return db, nil
}

// sqliteMigrations is append-only: never edit an applied migration, add a new version.
var sqliteMigrations = []migration{
	{
		version: 1,
		name:    "create_jobs_records_errors",
		statements: []string{
			`CREATE TABLE IF NOT EXISTS jobs (
    id         TEXT PRIMARY KEY,
    status     TEXT    NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    data       TEXT    NOT NULL
)`,
			`CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC, id DESC)`,
			`CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)`,
			`CREATE TABLE IF NOT EXISTS records (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    data   TEXT NOT NULL
)`,
			`CREATE INDEX IF NOT EXISTS idx_records_job ON records(job_id, id)`,
			`CREATE TABLE IF NOT EXISTS record_errors (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       TEXT    NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    source       TEXT    NOT NULL,
    record_index INTEGER NOT NULL,
    stage        TEXT    NOT NULL,
    message      TEXT    NOT NULL,
    payload      TEXT,
    occurred_at  INTEGER NOT NULL
)`,
			`CREATE INDEX IF NOT EXISTS idx_record_errors_job ON record_errors(job_id, id)`,
		},
	},
	{
		version: 2,
		name:    "job_coordination_and_api_keys",
		statements: []string{
			`ALTER TABLE jobs ADD COLUMN owner TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE jobs ADD COLUMN heartbeat_at INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE jobs ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0`,
			`CREATE INDEX IF NOT EXISTS idx_jobs_status_heartbeat ON jobs(status, heartbeat_at)`,
			`CREATE TABLE IF NOT EXISTS api_keys (
    id           TEXT PRIMARY KEY,
    name         TEXT    NOT NULL,
    prefix       TEXT    NOT NULL,
    key_hash     TEXT    NOT NULL UNIQUE,
    created_at   INTEGER NOT NULL,
    expires_at   INTEGER NOT NULL,
    revoked_at   INTEGER,
    last_used_at INTEGER
)`,
		},
	},
}
