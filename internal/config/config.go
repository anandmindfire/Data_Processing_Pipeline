package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config represents runtime application configuration.
type Config struct {
	ServerPort            string        `json:"server_port"`
	ServerAddr            string        `json:"server_addr"`
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

// Load loads configuration from an optional file and environment variables.
func Load() *Config {
	cfg := defaultConfiguration()
	loadFromFile(cfg)
	applyEnvironmentOverrides(cfg)
	return cfg
}

func defaultConfiguration() *Config {
	return &Config{
		ServerPort:            "8080",
		ServerAddr:            ":8080",
		DataDir:               "data",
		ExportDir:             "exports",
		MaxActiveJobs:         10,
		DefaultTimeoutSeconds: 300,
		MaxRequestBodyBytes:   2 << 20, // 2 MB
		RateLimitRPS:          50.0,
		RateLimitBurst:        100,
		LogLevel:              "info",
		LogFormat:             "text",
		ValidationWorkers:     4,
		TransformWorkers:      4,
		BufferSize:            64,
		ReadTimeout:           15 * time.Second,
		WriteTimeout:          30 * time.Second,
		IdleTimeout:           60 * time.Second,
	}
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

func applyEnvironmentOverrides(cfg *Config) {
	cfg.ServerPort = getEnv("PORT", cfg.ServerPort)
	cfg.ServerAddr = getEnv("ADDR", cfg.ServerAddr)
	if cfg.ServerAddr == ":8080" && cfg.ServerPort != "8080" {
		cfg.ServerAddr = ":" + cfg.ServerPort
	}
	cfg.DataDir = getEnv("DATA_DIR", cfg.DataDir)
	cfg.ExportDir = getEnv("EXPORT_DIR", cfg.ExportDir)
	cfg.MaxActiveJobs = getEnvInt("MAX_ACTIVE_JOBS", cfg.MaxActiveJobs)
	cfg.DefaultTimeoutSeconds = getEnvInt("DEFAULT_TIMEOUT_SECONDS", cfg.DefaultTimeoutSeconds)
	cfg.MaxRequestBodyBytes = getEnvInt64("MAX_REQUEST_BODY_BYTES", cfg.MaxRequestBodyBytes)
	cfg.RateLimitRPS = getEnvFloat("RATE_LIMIT_RPS", cfg.RateLimitRPS)
	cfg.RateLimitBurst = getEnvInt("RATE_LIMIT_BURST", cfg.RateLimitBurst)
	cfg.LogLevel = getEnv("LOG_LEVEL", cfg.LogLevel)
	cfg.LogFormat = getEnv("LOG_FORMAT", cfg.LogFormat)
	cfg.ValidationWorkers = getEnvInt("DEFAULT_VALIDATION_WORKERS", cfg.ValidationWorkers)
	cfg.TransformWorkers = getEnvInt("DEFAULT_TRANSFORM_WORKERS", cfg.TransformWorkers)
	cfg.BufferSize = getEnvInt("DEFAULT_BUFFER_SIZE", cfg.BufferSize)
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

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil && i > 0 {
			return i
		}
	}
	return defaultVal
}

func getEnvInt64(key string, defaultVal int64) int64 {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.ParseInt(val, 10, 64); err == nil && i > 0 {
			return i
		}
	}
	return defaultVal
}

func getEnvFloat(key string, defaultVal float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil && f > 0 {
			return f
		}
	}
	return defaultVal
}
