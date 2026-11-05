package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	// Registers the "pgx" database/sql driver for PostgreSQL.
	_ "github.com/jackc/pgx/v5/stdlib"
)

// PostgresDriver is the database/sql driver name registered by pgx.
const PostgresDriver = "pgx"

// migrationLockID is the pg_advisory_lock key that serializes migrations across replicas.
const migrationLockID = 727_100_001

// PostgresOptions tunes the connection pool. Zero values use sensible defaults.
type PostgresOptions struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// NewPostgresRepository connects to PostgreSQL (dsn such as
// "postgres://user:pass@host:5432/db?sslmode=require") and applies migrations under an advisory
// lock, so several replicas can start at the same time.
func NewPostgresRepository(ctx context.Context, dsn string, opts PostgresOptions) (*SQLRepository, error) {
	db, err := sql.Open(PostgresDriver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	db.SetMaxOpenConns(orDefault(opts.MaxOpenConns, 20))
	db.SetMaxIdleConns(orDefault(opts.MaxIdleConns, 10))
	if opts.ConnMaxLifetime <= 0 {
		opts.ConnMaxLifetime = 30 * time.Minute
	}
	db.SetConnMaxLifetime(opts.ConnMaxLifetime)
	db.SetConnMaxIdleTime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	r := &SQLRepository{db: db, d: postgresDialect}
	if err := r.migratePostgres(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return r, nil
}

func orDefault(v, def int) int {
	if v > 0 {
		return v
	}
	return def
}

// migratePostgres holds a session-level advisory lock on one connection while migrating.
func (r *SQLRepository) migratePostgres(ctx context.Context) error {
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrationLockID)
	}()
	return r.migrate(ctx, conn, postgresMigrations)
}

// postgresMigrations mirrors the SQLite schema. Timestamps are Unix nanoseconds (BIGINT) in both
// dialects so the shared repository code stays identical. Append-only.
var postgresMigrations = []migration{
	{
		version: 1,
		name:    "create_jobs_records_errors",
		statements: []string{
			`CREATE TABLE IF NOT EXISTS jobs (
    id         TEXT PRIMARY KEY,
    status     TEXT   NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    data       TEXT   NOT NULL
)`,
			`CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC, id DESC)`,
			`CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)`,
			`CREATE TABLE IF NOT EXISTS records (
    id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    data   TEXT NOT NULL
)`,
			`CREATE INDEX IF NOT EXISTS idx_records_job ON records(job_id, id)`,
			`CREATE TABLE IF NOT EXISTS record_errors (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id       TEXT    NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    source       TEXT    NOT NULL,
    record_index INTEGER NOT NULL,
    stage        TEXT    NOT NULL,
    message      TEXT    NOT NULL,
    payload      TEXT,
    occurred_at  BIGINT  NOT NULL
)`,
			`CREATE INDEX IF NOT EXISTS idx_record_errors_job ON record_errors(job_id, id)`,
		},
	},
	{
		version: 2,
		name:    "job_coordination_and_api_keys",
		statements: []string{
			`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS owner TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS heartbeat_at BIGINT NOT NULL DEFAULT 0`,
			`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS cancel_requested SMALLINT NOT NULL DEFAULT 0`,
			`CREATE INDEX IF NOT EXISTS idx_jobs_status_heartbeat ON jobs(status, heartbeat_at)`,
			`CREATE TABLE IF NOT EXISTS api_keys (
    id           TEXT PRIMARY KEY,
    name         TEXT   NOT NULL,
    prefix       TEXT   NOT NULL,
    key_hash     TEXT   NOT NULL UNIQUE,
    created_at   BIGINT NOT NULL,
    expires_at   BIGINT NOT NULL,
    revoked_at   BIGINT,
    last_used_at BIGINT
)`,
		},
	},
}
