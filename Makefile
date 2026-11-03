.PHONY: all build run test race vet clean docker-build docker-up docker-down

BINARY_NAME=pipeline-server
MAIN_PACKAGE=./cmd/server

all: test build

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

