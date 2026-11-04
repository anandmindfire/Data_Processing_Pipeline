package database

import (
	"context"
	"database/sql"
	"fmt"
)

// Migration represents a single schema migration script.
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// InitialMigrations defines core schema migrations for jobs, metrics, and error logs.
var InitialMigrations = []Migration{
	{
		Version: 1,
		Name:    "create_pipeline_jobs_table",
		SQL: `
CREATE TABLE IF NOT EXISTS pipeline_jobs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    config TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    results_path TEXT,
    aggregates TEXT
);
CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status ON pipeline_jobs(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_created_at ON pipeline_jobs(created_at);
`,
	},
	{
		Version: 2,
		Name:    "create_record_errors_table",
		SQL: `
CREATE TABLE IF NOT EXISTS record_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    source TEXT NOT NULL,
    record_index INTEGER NOT NULL,
    stage TEXT NOT NULL,
    message TEXT NOT NULL,
    payload TEXT,
    occurred_at TIMESTAMP NOT NULL,
    FOREIGN KEY(job_id) REFERENCES pipeline_jobs(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_record_errors_job ON record_errors(job_id);
`,
	},
}

// Migrate executes all pending migrations within a transaction.
func Migrate(ctx context.Context, db *sql.DB, migrations []Migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin migration transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	for _, m := range migrations {
		if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
			return fmt.Errorf("migration %d (%s) failed: %w", m.Version, m.Name, err)
		}
	}

	return tx.Commit()
}
