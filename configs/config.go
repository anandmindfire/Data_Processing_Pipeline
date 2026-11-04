package configs

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
}

// Load loads configuration from .env, optional JSON config file, and env overrides.
func Load() *Config {
	LoadEnvFile(".env")
	cfg := DefaultConfig()
	loadFromFile(cfg)
	ApplyEnvironmentOverrides(cfg)
	return cfg
}

func loadFromFile(cfg *Config) {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "configs/default.json"
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
