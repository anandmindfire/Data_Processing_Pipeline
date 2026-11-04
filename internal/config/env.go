package config

import (
	"bufio"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
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
	if err := scanner.Err(); err != nil {
		slog.Warn("failed scanning env file lines", "path", path, "error", err)
	}
}

// ApplyEnvironmentOverrides updates Config with environment variable overrides.
func ApplyEnvironmentOverrides(cfg *Config) {
	cfg.ServerPort = GetEnvFirst([]string{"PORT", "SERVER_PORT"}, cfg.ServerPort)
	cfg.ServerAddr = GetEnvFirst([]string{"ADDR", "SERVER_ADDR"}, cfg.ServerAddr)
	syncServerAddress(cfg)

	cfg.APIBaseURL = GetEnvFirst([]string{"API_BASE_URL", "BASE_URL", "SERVER_URL"}, cfg.APIBaseURL)
	if cfg.APIBaseURL == "http://localhost:8080" && cfg.ServerPort != "8080" {
		cfg.APIBaseURL = "http://localhost:" + cfg.ServerPort
	}

	cfg.APIBasePath = GetEnvFirst([]string{"API_BASE_PATH", "API_PREFIX"}, cfg.APIBasePath)
	if cfg.APIBasePath != "" {
		cfg.APIBasePath = "/" + strings.Trim(cfg.APIBasePath, "/")
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
	cfg.ShutdownTimeout = GetEnvDuration("SHUTDOWN_TIMEOUT", cfg.ShutdownTimeout)
	cfg.ReadTimeout = GetEnvDuration("READ_TIMEOUT", cfg.ReadTimeout)
	cfg.WriteTimeout = GetEnvDuration("WRITE_TIMEOUT", cfg.WriteTimeout)
	cfg.IdleTimeout = GetEnvDuration("IDLE_TIMEOUT", cfg.IdleTimeout)

	cfg.InputDirs = GetEnvList("INPUT_DIRS", cfg.InputDirs)
	cfg.AllowPrivateSources = GetEnvBool("ALLOW_PRIVATE_SOURCES", cfg.AllowPrivateSources)
	cfg.MaxWorkersPerStage = GetEnvInt("MAX_WORKERS_PER_STAGE", cfg.MaxWorkersPerStage)
	cfg.MaxBufferSize = GetEnvInt("MAX_BUFFER_SIZE", cfg.MaxBufferSize)
	cfg.MaxBatchSize = GetEnvInt("MAX_BATCH_SIZE", cfg.MaxBatchSize)
	cfg.MaxJobTimeoutSeconds = GetEnvInt("MAX_JOB_TIMEOUT_SECONDS", cfg.MaxJobTimeoutSeconds)
	cfg.MaxSourcesPerJob = GetEnvInt("MAX_SOURCES_PER_JOB", cfg.MaxSourcesPerJob)
	cfg.MaxExportsPerJob = GetEnvInt("MAX_EXPORTS_PER_JOB", cfg.MaxExportsPerJob)
}

// GetEnvDuration retrieves a Go duration (e.g. "30s") with fallback default.
func GetEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		if d, err := time.ParseDuration(val); err == nil && d > 0 {
			return d
		}
		slog.Warn("ignoring invalid duration environment variable", "key", key, "value", val)
	}
	return defaultVal
}

// GetEnvBool retrieves a boolean environment variable with fallback default.
func GetEnvBool(key string, defaultVal bool) bool {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		if b, err := strconv.ParseBool(val); err == nil {
			return b
		}
	}
	return defaultVal
}

// GetEnvList retrieves a comma-separated list environment variable with fallback default.
func GetEnvList(key string, defaultVal []string) []string {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return defaultVal
	}
	var out []string
	for _, part := range strings.Split(val, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// syncServerAddress synchronizes ServerPort and ServerAddr so host-port binds remain consistent.
func syncServerAddress(cfg *Config) {
	addrEnv := GetEnvFirst([]string{"ADDR", "SERVER_ADDR"}, "")
	portEnv := GetEnvFirst([]string{"PORT", "SERVER_PORT"}, "")

	if addrEnv != "" && portEnv == "" {
		if _, p, err := net.SplitHostPort(cfg.ServerAddr); err == nil && p != "" {
			cfg.ServerPort = p
		} else if strings.HasPrefix(cfg.ServerAddr, ":") {
			cfg.ServerPort = strings.TrimPrefix(cfg.ServerAddr, ":")
		}
	} else if portEnv != "" && addrEnv == "" {
		if host, _, err := net.SplitHostPort(cfg.ServerAddr); err == nil {
			cfg.ServerAddr = net.JoinHostPort(host, cfg.ServerPort)
		} else {
			cfg.ServerAddr = ":" + cfg.ServerPort
		}
	} else if cfg.ServerAddr == ":8080" && cfg.ServerPort != "8080" {
		cfg.ServerAddr = ":" + cfg.ServerPort
	}
}

// GetEnvFirst retrieves the first set environment variable from keys or falls back to defaultVal.
func GetEnvFirst(keys []string, defaultVal string) string {
	for _, key := range keys {
		if val := strings.TrimSpace(os.Getenv(key)); val != "" {
			return val
		}
	}
	return defaultVal
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
