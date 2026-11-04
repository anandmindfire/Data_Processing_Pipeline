.PHONY: all build run test race vet fmt lint hooks check clean docker-build docker-up docker-down load-test

BINARY_NAME=pipeline-server
MAIN_PACKAGE=./cmd/server

all: check build

build:
	@echo "==> Building binary $(BINARY_NAME)..."
	go build -trimpath -ldflags="-s -w" -o bin/$(BINARY_NAME) $(MAIN_PACKAGE)

run:
	@echo "==> Running $(BINARY_NAME)..."
	go run $(MAIN_PACKAGE)

test:
	@echo "==> Running test suite..."
	go test -v ./...

race:
	@echo "==> Running test suite with race detector..."
	go test -race -count=1 ./...

vet:
	@echo "==> Running static analysis..."
	go vet ./...

fmt:
	@echo "==> Formatting Go files..."
	@if command -v gofumpt >/dev/null 2>&1; then \
		gofumpt -w .; \
	else \
		go fmt ./...; \
	fi

lint:
	@echo "==> Running linter..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		go vet ./...; \
	fi

hooks:
	@echo "==> Configuring git hooks in .githooks/..."
	git config core.hooksPath .githooks
	chmod +x .githooks/*
	@echo "==> Git hooks activated successfully!"

check: fmt vet race
	@echo "==> All checks passed!"

clean:
	@echo "==> Cleaning build artifacts..."
	rm -rf bin/ exports/ data/

docker-build:
	@echo "==> Building Docker image..."
	docker build -t data-processing-pipeline:latest .

docker-up:
	@echo "==> Starting Docker containers..."
	docker compose up -d

docker-down:
	@echo "==> Stopping Docker containers..."
	docker compose down

load-test:
	@echo "==> Running load test against pipeline server..."
	./scripts/load_test.sh
