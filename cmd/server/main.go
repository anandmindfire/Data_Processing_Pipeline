package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/bootstrap"
)

// resolveHealthCheckURL returns the URL the container health probe calls. It targets the local
// listener (derived from PORT/ADDR) rather than API_BASE_URL, which is the public address and is
// usually not reachable from inside the container. HEALTHCHECK_URL overrides it explicitly.
func resolveHealthCheckURL() string {
	if rawURL := os.Getenv("HEALTHCHECK_URL"); rawURL != "" {
		return rawURL
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = os.Getenv("SERVER_PORT")
	}

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = os.Getenv("SERVER_ADDR")
	}

	if port == "" && addr != "" {
		if _, p, err := net.SplitHostPort(addr); err == nil && p != "" {
			port = p
		} else if strings.HasPrefix(addr, ":") {
			port = strings.TrimPrefix(addr, ":")
		}
	}

	if port == "" {
		port = "8080"
	}

	return fmt.Sprintf("http://127.0.0.1:%s/healthz", port)
}

func runHealthCheck() {
	targetURL := resolveHealthCheckURL()
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(targetURL)
	if err != nil || resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
	os.Exit(0)
}

func handleFlags() bool {
	if len(os.Args) <= 1 {
		return false
	}
	switch os.Args[1] {
	case "--help", "-h", "-help", "help":
		fmt.Println("Usage: pipeline-server [flags]")
		fmt.Println("Flags:")
		fmt.Println("  --help, -h          Show help message")
		fmt.Println("  --version, -v       Show version")
		fmt.Println("  --healthcheck       Run container health probe")
		return true
	case "--version", "-v", "-version", "version":
		fmt.Println("pipeline-server version 1.0.0")
		return true
	case "--healthcheck", "-healthcheck":
		runHealthCheck()
		return true
	}
	return false
}

func main() {
	if handleFlags() {
		return
	}

	app, err := bootstrap.NewApp()
	if err != nil {
		slog.Error("bootstrap failed", "error", err)
		os.Exit(1)
	}

	serverErr := app.Start()

	// Wait for an OS shutdown signal or a listener failure.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigChan:
		slog.Info("received signal, initiating graceful shutdown", "signal", sig.String())
	case err := <-serverErr:
		slog.Error("http server failed", "error", err)
		ctx, cancel := context.WithTimeout(context.Background(), app.Config.ShutdownTimeout)
		_ = app.Stop(ctx)
		cancel()
		os.Exit(1)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), app.Config.ShutdownTimeout)
	defer cancel()

	if err := app.Stop(shutdownCtx); err != nil {
		slog.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}
	slog.Info("server stopped gracefully")
}
