package config

import (
	"fmt"
	"os"
	"strconv"
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
	ReadTimeout           time.Duration `json:"read_timeout"`
	WriteTimeout          time.Duration `json:"write_timeout"`
	IdleTimeout           time.Duration `json:"idle_timeout"`
}

// Load loads configuration from environment variables with safe defaults.
func Load() *Config {
	addr := getEnv("ADDR", ":8080")
	port := getEnv("PORT", "8080")
	if addr == ":8080" && port != "8080" {
		addr = ":" + port
	}

	return &Config{
		ServerPort:            port,
		ServerAddr:            addr,
		DataDir:               getEnv("DATA_DIR", "data"),
		ExportDir:             getEnv("EXPORT_DIR", "exports"),
		MaxActiveJobs:         getEnvInt("MAX_ACTIVE_JOBS", 10),
		DefaultTimeoutSeconds: getEnvInt("DEFAULT_TIMEOUT_SECONDS", 300),
		MaxRequestBodyBytes:   getEnvInt64("MAX_REQUEST_BODY_BYTES", 2<<20), // 2 MB
		RateLimitRPS:          getEnvFloat("RATE_LIMIT_RPS", 50.0),
		RateLimitBurst:        getEnvInt("RATE_LIMIT_BURST", 100),
		ReadTimeout:           15 * time.Second,
		WriteTimeout:          30 * time.Second,
		IdleTimeout:           60 * time.Second,
	}
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
	return nil
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

