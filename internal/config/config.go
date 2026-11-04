package config

import (
	"encoding/json"
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
	MaxWorkersPerStage   int `json:"max_workers_per_stage"`
	MaxBufferSize        int `json:"max_buffer_size"`
	MaxBatchSize         int `json:"max_batch_size"`
	MaxJobTimeoutSeconds int `json:"max_job_timeout_seconds"`
	MaxSourcesPerJob     int `json:"max_sources_per_job"`
	MaxExportsPerJob     int `json:"max_exports_per_job"`
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

	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	_ = json.Unmarshal(data, cfg)
}

// Validate verifies that the configuration values are sound.
func (c *Config) Validate() error {
	if c.MaxActiveJobs <= 0 {
		return fmt.Errorf("max_active_jobs must be greater than 0")
	}
	if c.DefaultTimeoutSeconds <= 0 {
		return fmt.Errorf("default_timeout_seconds must be greater than 0")
	}
	if c.RateLimitRPS <= 0 {
		return fmt.Errorf("rate_limit_rps must be greater than 0")
	}
	if c.RateLimitBurst <= 0 {
		return fmt.Errorf("rate_limit_burst must be greater than 0")
	}
	if c.MaxWorkersPerStage <= 0 || c.MaxBufferSize <= 0 || c.MaxBatchSize <= 0 ||
		c.MaxJobTimeoutSeconds <= 0 || c.MaxSourcesPerJob <= 0 || c.MaxExportsPerJob <= 0 {
		return fmt.Errorf("per-job limits must be greater than 0")
	}
	if c.ValidationWorkers > c.MaxWorkersPerStage || c.TransformWorkers > c.MaxWorkersPerStage {
		return fmt.Errorf("default worker counts must not exceed max_workers_per_stage (%d)", c.MaxWorkersPerStage)
	}
	if c.BufferSize > c.MaxBufferSize {
		return fmt.Errorf("default buffer_size must not exceed max_buffer_size (%d)", c.MaxBufferSize)
	}
	if c.DefaultTimeoutSeconds > c.MaxJobTimeoutSeconds {
		return fmt.Errorf("default_timeout_seconds must not exceed max_job_timeout_seconds (%d)", c.MaxJobTimeoutSeconds)
	}
	if strings.TrimSpace(c.DataDir) == "" || strings.TrimSpace(c.ExportDir) == "" {
		return fmt.Errorf("data_dir and export_dir are required")
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
