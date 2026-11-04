package bootstrap

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/config"
)

func TestAppLifecycle(t *testing.T) {
	tempDir := t.TempDir()

	// Find an available port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skip("unable to bind ephemeral port:", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	// Set env for NewApp
	t.Setenv("ADDR", addr)
	t.Setenv("DATA_DIR", filepath.Join(tempDir, "data"))
	t.Setenv("EXPORT_DIR", filepath.Join(tempDir, "exports"))
	t.Setenv("INPUT_DIRS", filepath.Join(tempDir, "inputs"))
	_ = os.MkdirAll(filepath.Join(tempDir, "inputs"), 0o755)

	app, err := NewApp()
	if err != nil {
		t.Fatalf("NewApp failed: %v", err)
	}
	if app == nil || app.Server == nil {
		t.Fatal("expected non-nil App and Server")
	}

	errCh := app.Start()
	// Server should be running, errCh should not immediately produce error
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected start error: %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		// Running happily
	}

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := app.Stop(ctx); err != nil {
		t.Fatalf("app.Stop failed: %v", err)
	}
}

func TestPolicy(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.InputDirs = []string{"/tmp/input"}
	cfg.ExportDir = "/tmp/export"
	cfg.MaxWorkersPerStage = 8
	cfg.MaxBufferSize = 500
	cfg.MaxBatchSize = 100
	cfg.MaxJobTimeoutSeconds = 300
	cfg.MaxSourcesPerJob = 5
	cfg.MaxExportsPerJob = 5

	policy := Policy(cfg)
	if len(policy.InputDirs) != 1 || policy.InputDirs[0] != "/tmp/input" {
		t.Errorf("expected input dirs [/tmp/input], got %v", policy.InputDirs)
	}
	if policy.ExportDir != "/tmp/export" {
		t.Errorf("expected export dir /tmp/export, got %s", policy.ExportDir)
	}
	if policy.MaxWorkers != 8 {
		t.Errorf("expected MaxWorkers 8, got %d", policy.MaxWorkers)
	}
}
