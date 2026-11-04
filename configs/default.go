package configs

import "time"

// DefaultConfig returns a new Config instance initialized with robust production defaults.
func DefaultConfig() *Config {
	return &Config{
		ServerPort:            "8080",
		ServerAddr:            ":8080",
		DataDir:               "data",
		ExportDir:             "exports",
		MaxActiveJobs:         10,
		DefaultTimeoutSeconds: 300,
		MaxRequestBodyBytes:   2 << 20, // 2 MB
		RateLimitRPS:          50.0,
		RateLimitBurst:        100,
		LogLevel:              "info",
		LogFormat:             "text",
		ValidationWorkers:     4,
		TransformWorkers:      4,
		BufferSize:            64,
		ReadTimeout:           15 * time.Second,
		WriteTimeout:          30 * time.Second,
		IdleTimeout:           60 * time.Second,
	}
}
