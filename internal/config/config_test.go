package config

import (
	"os"
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
	defer func() {
		_ = os.Unsetenv("ADDR")
		_ = os.Unsetenv("DATA_DIR")
		_ = os.Unsetenv("MAX_ACTIVE_JOBS")
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
}
