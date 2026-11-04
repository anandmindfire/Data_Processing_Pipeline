package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func TestConfigLoadDefaults(t *testing.T) {
	cfg := Load()
	if cfg.ServerAddr == "" {
		t.Fatal("expected default server addr")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config validation failed: %v", err)
	}
}

func TestConfigEnvOverrides(t *testing.T) {
	_ = os.Setenv("ADDR", ":9999")
	_ = os.Setenv("DATA_DIR", "/custom/data")
	_ = os.Setenv("MAX_ACTIVE_JOBS", "25")
	_ = os.Setenv("LOG_LEVEL", "debug")
	_ = os.Setenv("LOG_FORMAT", "json")
	defer func() {
		_ = os.Unsetenv("ADDR")
		_ = os.Unsetenv("DATA_DIR")
		_ = os.Unsetenv("MAX_ACTIVE_JOBS")
		_ = os.Unsetenv("LOG_LEVEL")
		_ = os.Unsetenv("LOG_FORMAT")
	}()

	cfg := Load()
	if cfg.ServerAddr != ":9999" {
		t.Fatalf("expected :9999, got %s", cfg.ServerAddr)
	}
	if cfg.DataDir != "/custom/data" {
		t.Fatalf("expected /custom/data, got %s", cfg.DataDir)
	}
	if cfg.MaxActiveJobs != 25 {
		t.Fatalf("expected 25, got %d", cfg.MaxActiveJobs)
	}
	if cfg.LogLevel != "debug" {
		t.Fatalf("expected debug log level, got %s", cfg.LogLevel)
	}

	logger := cfg.InitLogger()
	if logger == nil {
		t.Fatal("expected initialized logger")
	}
}

func TestConfigFileLoading(t *testing.T) {
	dir := t.TempDir()
	confPath := filepath.Join(dir, "test_config.json")
	_ = os.WriteFile(confPath, []byte(`{"server_port":"9090","max_active_jobs":42}`), 0644)

	_ = os.Setenv("CONFIG_PATH", confPath)
	defer func() {
		_ = os.Unsetenv("CONFIG_PATH")
	}()

	cfg := Load()
	if cfg.ServerPort != "9090" {
		t.Fatalf("expected 9090 from file, got %s", cfg.ServerPort)
	}
	if cfg.MaxActiveJobs != 42 {
		t.Fatalf("expected 42 from file, got %d", cfg.MaxActiveJobs)
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := Load()
	cfg.MaxActiveJobs = -1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on negative max active jobs")
	}

	cfg = Load()
	cfg.DefaultTimeoutSeconds = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on zero timeout")
	}

	cfg = Load()
	cfg.RateLimitRPS = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on zero rate limit RPS")
	}
}

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"info", slog.LevelInfo},
		{"unknown", slog.LevelInfo},
	}
	for _, tc := range tests {
		if got := parseLogLevel(tc.input); got != tc.expected {
			t.Errorf("parseLogLevel(%q) = %v, expected %v", tc.input, got, tc.expected)
		}
	}
}
