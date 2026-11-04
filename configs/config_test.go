package configs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigsLoadDefaults(t *testing.T) {
	cfg := Load()
	if cfg.ServerAddr == "" {
		t.Fatal("expected default server addr")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config validation failed: %v", err)
	}
}

func TestConfigsEnvOverrides(t *testing.T) {
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

func TestConfigsFileLoading(t *testing.T) {
	dir := t.TempDir()
	confPath := filepath.Join(dir, "test_config.json")
	_ = os.WriteFile(confPath, []byte(`{"server_port":"9090","max_active_jobs":42}`), 0o600)

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

func TestAPIBaseURLAndPortConfig(t *testing.T) {
	t.Run("explicit port derives address and base url", func(t *testing.T) {
		_ = os.Setenv("PORT", "9095")
		defer func() { _ = os.Unsetenv("PORT") }()

		cfg := Load()
		if cfg.ServerPort != "9095" {
			t.Errorf("expected ServerPort 9095, got %s", cfg.ServerPort)
		}
		if cfg.ServerAddr != ":9095" {
			t.Errorf("expected ServerAddr :9095, got %s", cfg.ServerAddr)
		}
		if cfg.APIBaseURL != "http://localhost:9095" {
			t.Errorf("expected APIBaseURL http://localhost:9095, got %s", cfg.APIBaseURL)
		}
	})

	t.Run("explicit api base url and prefix override", func(t *testing.T) {
		_ = os.Setenv("API_BASE_URL", "https://api.example.com")
		_ = os.Setenv("API_BASE_PATH", "v2/custom")
		defer func() {
			_ = os.Unsetenv("API_BASE_URL")
			_ = os.Unsetenv("API_BASE_PATH")
		}()

		cfg := Load()
		if cfg.APIBaseURL != "https://api.example.com" {
			t.Errorf("expected APIBaseURL https://api.example.com, got %s", cfg.APIBaseURL)
		}
		if cfg.APIBasePath != "/v2/custom" {
			t.Errorf("expected APIBasePath /v2/custom, got %s", cfg.APIBasePath)
		}
	})
}
