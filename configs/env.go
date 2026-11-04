package configs

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// LoadEnvFile reads a key=value formatted .env file and sets the variables in the process environment.
func LoadEnvFile(path string) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer func() {
		_ = file.Close()
	}()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			val = strings.Trim(val, `"'`)
			if _, exists := os.LookupEnv(key); !exists {
				_ = os.Setenv(key, val)
			}
		}
	}
}

// ApplyEnvironmentOverrides updates Config with environment variable overrides.
func ApplyEnvironmentOverrides(cfg *Config) {
	cfg.ServerPort = GetEnv("PORT", cfg.ServerPort)
	cfg.ServerAddr = GetEnv("ADDR", cfg.ServerAddr)
	if cfg.ServerAddr == ":8080" && cfg.ServerPort != "8080" {
		cfg.ServerAddr = ":" + cfg.ServerPort
	}
	cfg.APIKey = GetEnv("API_KEY", cfg.APIKey)
	cfg.DataDir = GetEnv("DATA_DIR", cfg.DataDir)
	cfg.ExportDir = GetEnv("EXPORT_DIR", cfg.ExportDir)
	cfg.MaxActiveJobs = GetEnvInt("MAX_ACTIVE_JOBS", cfg.MaxActiveJobs)
	cfg.DefaultTimeoutSeconds = GetEnvInt("DEFAULT_TIMEOUT_SECONDS", cfg.DefaultTimeoutSeconds)
	cfg.MaxRequestBodyBytes = GetEnvInt64("MAX_REQUEST_BODY_BYTES", cfg.MaxRequestBodyBytes)
	cfg.RateLimitRPS = GetEnvFloat("RATE_LIMIT_RPS", cfg.RateLimitRPS)
	cfg.RateLimitBurst = GetEnvInt("RATE_LIMIT_BURST", cfg.RateLimitBurst)
	cfg.LogLevel = GetEnv("LOG_LEVEL", cfg.LogLevel)
	cfg.LogFormat = GetEnv("LOG_FORMAT", cfg.LogFormat)
	cfg.ValidationWorkers = GetEnvInt("DEFAULT_VALIDATION_WORKERS", cfg.ValidationWorkers)
	cfg.TransformWorkers = GetEnvInt("DEFAULT_TRANSFORM_WORKERS", cfg.TransformWorkers)
	cfg.BufferSize = GetEnvInt("DEFAULT_BUFFER_SIZE", cfg.BufferSize)
}

// GetEnv retrieves a string environment variable with fallback default.
func GetEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// GetEnvInt retrieves an integer environment variable with fallback default.
func GetEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil && i > 0 {
			return i
		}
	}
	return defaultVal
}

// GetEnvInt64 retrieves an int64 environment variable with fallback default.
func GetEnvInt64(key string, defaultVal int64) int64 {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.ParseInt(val, 10, 64); err == nil && i > 0 {
			return i
		}
	}
	return defaultVal
}

// GetEnvFloat retrieves a float64 environment variable with fallback default.
func GetEnvFloat(key string, defaultVal float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil && f > 0 {
			return f
		}
	}
	return defaultVal
}
