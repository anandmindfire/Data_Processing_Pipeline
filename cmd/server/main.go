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

func resolveHealthCheckURL() string {
	if rawURL := os.Getenv("API_BASE_URL"); rawURL != "" {
		return strings.TrimRight(rawURL, "/") + "/healthz"
	}
	if rawURL := os.Getenv("SERVER_URL"); rawURL != "" {
		return strings.TrimRight(rawURL, "/") + "/healthz"
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

	app.Start()

	// Wait for OS shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan

	slog.Info("received signal, initiating graceful shutdown", "signal", sig.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := app.Stop(shutdownCtx); err != nil {
		slog.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}
	slog.Info("server stopped gracefully")
}
