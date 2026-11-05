package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

// Config represents runtime application configuration.
type Config struct {
	ServerPort            string        `json:"server_port"`
	ServerAddr            string        `json:"server_addr"`
	APIBaseURL            string        `json:"api_base_url"`
	APIBasePath           string        `json:"api_base_path"`
	APIKey                string        `json:"api_key"`
	DataDir               string        `json:"data_dir"`
	ExportDir             string        `json:"export_dir"`
	MaxActiveJobs         int           `json:"max_active_jobs"`
	DefaultTimeoutSeconds int           `json:"default_timeout_seconds"`
	MaxRequestBodyBytes   int64         `json:"max_request_body_bytes"`
	RateLimitRPS          float64       `json:"rate_limit_rps"`
	RateLimitBurst        int           `json:"rate_limit_burst"`
	LogLevel              string        `json:"log_level"`
	LogFormat             string        `json:"log_format"`
	ValidationWorkers     int           `json:"validation_workers"`
	TransformWorkers      int           `json:"transform_workers"`
	BufferSize            int           `json:"buffer_size"`
	ReadTimeout           time.Duration `json:"read_timeout"`
	WriteTimeout          time.Duration `json:"write_timeout"`
	IdleTimeout           time.Duration `json:"idle_timeout"`
	ShutdownTimeout       time.Duration `json:"shutdown_timeout"`

	// Security boundaries for job specifications.
	InputDirs           []string `json:"input_dirs"`
	AllowPrivateSources bool     `json:"allow_private_sources"`

	// Per-job resource limits enforced when a job is submitted.
	MaxWorkersPerStage    int   `json:"max_workers_per_stage"`
	MaxBufferSize         int   `json:"max_buffer_size"`
	MaxBatchSize          int   `json:"max_batch_size"`
	MaxJobTimeoutSeconds  int   `json:"max_job_timeout_seconds"`
	MaxSourcesPerJob      int   `json:"max_sources_per_job"`
	MaxExportsPerJob      int   `json:"max_exports_per_job"`
	MaxSourceBytes        int64 `json:"max_source_bytes"` // per source (file or HTTP body)
	MaxAggregationWorkers int   `json:"max_aggregation_workers"`

	// HTTP edge hardening.
	RequestTimeout     time.Duration `json:"request_timeout"`      // per API request
	TrustedProxies     []string      `json:"trusted_proxies"`      // CIDRs allowed to set X-Forwarded-For
	CORSAllowedOrigins []string      `json:"cors_allowed_origins"` // empty = CORS disabled; "*" = any origin
	MetricsPublic      bool          `json:"metrics_public"`       // serve /metrics without the API key
	DocsEnabled        bool          `json:"docs_enabled"`         // serve Swagger UI and the OpenAPI spec

	// Storage backend. Empty DatabaseURL = embedded SQLite in DataDir (single instance);
	// a postgres:// URL enables multi-replica deployments.
	DatabaseURL    string        `json:"-"` // may contain credentials; never serialized
	DBMaxOpenConns int           `json:"db_max_open_conns"`
	DBMaxIdleConns int           `json:"db_max_idle_conns"`
	DBConnLifetime time.Duration `json:"db_conn_max_lifetime"`
	InstanceID     string        `json:"instance_id"` // replica identity (default hostname-random)
	JobLease       time.Duration `json:"job_lease"`   // jobs not heartbeated for this long are failed

	// API key management.
	APIKeyDefaultTTL time.Duration `json:"api_key_default_ttl"`
	APIKeyMaxTTL     time.Duration `json:"api_key_max_ttl"`
	MaxAPIKeys       int           `json:"max_api_keys"`

	// Data lifecycle.
	JobRetention time.Duration `json:"job_retention"` // finished jobs older than this are purged (0 = keep forever)
}

// minAPIKeyLength is the shortest API key accepted, to rule out trivially guessable keys.
const minAPIKeyLength = 16

// APIKeys returns the configured API keys. API_KEY may hold a comma-separated list so keys can
// be rotated without downtime (add the new key, roll clients, remove the old key).
func (c *Config) APIKeys() []string {
	var keys []string
	for _, k := range strings.Split(c.APIKey, ",") {
		if k = strings.TrimSpace(k); k != "" {
			keys = append(keys, k)
		}
	}
	return keys
}

// Load loads configuration from .env, optional JSON config file, and env overrides.
func Load() *Config {
	LoadEnvFile(".env")
	cfg := DefaultConfig()
	loadFromFile(cfg)
	ApplyEnvironmentOverrides(cfg)
	return cfg
}

// loadFromFile reads default configurations from a JSON file path if accessible.
func loadFromFile(cfg *Config) {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		for _, candidate := range []string{"internal/config/default.json", "configs/default.json"} {
			if _, err := os.Stat(candidate); err == nil {
				path = candidate
				break
			}
		}
	}
	if path == "" {
		path = "internal/config/default.json"
	}

	data, err := os.ReadFile(path) //nolint:gosec // CONFIG_PATH is operator-controlled configuration, not request input
	if err != nil {
		return
	}
	_ = json.Unmarshal(data, cfg)
}

// Validate verifies that the configuration values are sound.
func (c *Config) Validate() error {
	return errors.Join(c.validateSecurity(), c.validateServer(), c.validateJobLimits())
}

func (c *Config) validateSecurity() error {
	for _, k := range c.APIKeys() {
		if len(k) < minAPIKeyLength {
			return fmt.Errorf("every API key must be at least %d characters long", minAPIKeyLength)
		}
	}
	if strings.TrimSpace(c.DataDir) == "" || strings.TrimSpace(c.ExportDir) == "" {
		return errors.New("data_dir and export_dir are required")
	}
	return nil
}

// UsesPostgres reports whether the shared PostgreSQL store is configured.
func (c *Config) UsesPostgres() bool {
	return strings.HasPrefix(c.DatabaseURL, "postgres://") || strings.HasPrefix(c.DatabaseURL, "postgresql://")
}

func (c *Config) validateServer() error {
	switch {
	case c.DatabaseURL != "" && !c.UsesPostgres():
		return errors.New("DATABASE_URL must be a postgres:// or postgresql:// URL")
	case c.JobLease < 5*time.Second:
		return errors.New("job_lease must be at least 5s")
	case c.APIKeyDefaultTTL <= 0 || c.APIKeyMaxTTL < c.APIKeyDefaultTTL || c.MaxAPIKeys <= 0:
		return errors.New("api_key_default_ttl must be > 0 and <= api_key_max_ttl, and max_api_keys > 0")
	case c.MaxActiveJobs <= 0:
		return errors.New("max_active_jobs must be greater than 0")
	case c.RateLimitRPS <= 0 || c.RateLimitBurst <= 0:
		return errors.New("rate_limit_rps and rate_limit_burst must be greater than 0")
	case c.RequestTimeout <= 0:
		return errors.New("request_timeout must be greater than 0")
	case c.JobRetention < 0:
		return errors.New("job_retention must not be negative")
	}
	return nil
}

func (c *Config) validateJobLimits() error {
	switch {
	case c.MaxWorkersPerStage <= 0 || c.MaxBufferSize <= 0 || c.MaxBatchSize <= 0 || c.MaxJobTimeoutSeconds <= 0 ||
		c.MaxSourcesPerJob <= 0 || c.MaxExportsPerJob <= 0 || c.MaxSourceBytes <= 0 || c.MaxAggregationWorkers <= 0:
		return errors.New("per-job limits must be greater than 0")
	case c.DefaultTimeoutSeconds <= 0:
		return errors.New("default_timeout_seconds must be greater than 0")
	case c.ValidationWorkers > c.MaxWorkersPerStage || c.TransformWorkers > c.MaxWorkersPerStage:
		return fmt.Errorf("default worker counts must not exceed max_workers_per_stage (%d)", c.MaxWorkersPerStage)
	case c.BufferSize > c.MaxBufferSize:
		return fmt.Errorf("default buffer_size must not exceed max_buffer_size (%d)", c.MaxBufferSize)
	case c.DefaultTimeoutSeconds > c.MaxJobTimeoutSeconds:
		return fmt.Errorf("default_timeout_seconds must not exceed max_job_timeout_seconds (%d)", c.MaxJobTimeoutSeconds)
	}
	return nil
}

// InitLogger configures the global slog logger according to Config settings.
func (c *Config) InitLogger() *slog.Logger {
	level := parseLogLevel(c.LogLevel)
	var handler slog.Handler

	opts := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(c.LogFormat, "json") {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

// parseLogLevel converts a case-insensitive string level into a slog.Level value.
func parseLogLevel(lvl string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(lvl)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
