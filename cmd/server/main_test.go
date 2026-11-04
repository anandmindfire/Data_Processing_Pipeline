package main

import (
	"os"
	"testing"
)

func TestHandleFlags(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	tests := []struct {
		name     string
		args     []string
		expected bool
	}{
		{
			name:     "no args",
			args:     []string{"pipeline-server"},
			expected: false,
		},
		{
			name:     "help long",
			args:     []string{"pipeline-server", "--help"},
			expected: true,
		},
		{
			name:     "help short",
			args:     []string{"pipeline-server", "-h"},
			expected: true,
		},
		{
			name:     "version long",
			args:     []string{"pipeline-server", "--version"},
			expected: true,
		},
		{
			name:     "version short",
			args:     []string{"pipeline-server", "-v"},
			expected: true,
		},
		{
			name:     "unknown arg",
			args:     []string{"pipeline-server", "--unknown"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Args = tt.args
			got := handleFlags()
			if got != tt.expected {
				t.Errorf("handleFlags() for %v = %v; want %v", tt.args, got, tt.expected)
			}
		})
	}
}

func TestResolveHealthCheckURL(t *testing.T) {
	// Clean env before and after
	envVars := []string{"API_BASE_URL", "SERVER_URL", "PORT", "SERVER_PORT", "ADDR", "SERVER_ADDR"}
	clearEnv := func() {
		for _, v := range envVars {
			_ = os.Unsetenv(v)
		}
	}
	clearEnv()
	defer clearEnv()

	tests := []struct {
		name     string
		env      map[string]string
		expected string
	}{
		{
			name:     "default fallback when nothing set",
			env:      map[string]string{},
			expected: "http://127.0.0.1:8080/healthz",
		},
		{
			name:     "PORT specified",
			env:      map[string]string{"PORT": "9090"},
			expected: "http://127.0.0.1:9090/healthz",
		},
		{
			name:     "SERVER_PORT specified",
			env:      map[string]string{"SERVER_PORT": "7070"},
			expected: "http://127.0.0.1:7070/healthz",
		},
		{
			name:     "ADDR with colon prefix",
			env:      map[string]string{"ADDR": ":6060"},
			expected: "http://127.0.0.1:6060/healthz",
		},
		{
			name:     "ADDR with host and port",
			env:      map[string]string{"SERVER_ADDR": "0.0.0.0:5050"},
			expected: "http://127.0.0.1:5050/healthz",
		},
		{
			name:     "API_BASE_URL takes precedence",
			env:      map[string]string{"API_BASE_URL": "http://custom-domain.internal:8000"},
			expected: "http://custom-domain.internal:8000/healthz",
		},
		{
			name:     "SERVER_URL fallback",
			env:      map[string]string{"SERVER_URL": "http://app.cluster.local:9000/"},
			expected: "http://app.cluster.local:9000/healthz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv()
			for k, v := range tt.env {
				_ = os.Setenv(k, v)
			}
			got := resolveHealthCheckURL()
			if got != tt.expected {
				t.Errorf("resolveHealthCheckURL() = %q; want %q", got, tt.expected)
			}
		})
	}
}
