# Developer and CI entry points. Run `make help` for a list of targets.
SHELL := /bin/bash
.DEFAULT_GOAL := help

BINARY      := pipeline-server
PKG         := github.com/anandmindfire/data-processing-pipeline
IMAGE       ?= data-processing-pipeline
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE  ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
COVER_MIN   ?= 75
LDFLAGS     := -s -w \
	-X $(PKG)/internal/buildinfo.Version=$(VERSION) \
	-X $(PKG)/internal/buildinfo.Commit=$(COMMIT) \
	-X $(PKG)/internal/buildinfo.Date=$(BUILD_DATE)

GOLANGCI_LINT_VERSION ?= v2.15.0
GOVULNCHECK_VERSION   ?= latest

.PHONY: help build run test race cover vet fmt lint vuln check tools hooks clean \
        docker-build docker-up docker-down docker-smoke load-test

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the server binary into bin/ with version metadata
	go build -trimpath -ldflags="$(LDFLAGS)" -o bin/$(BINARY) ./cmd/server

run: ## Run the server locally
	go run ./cmd/server

test: ## Run all tests
	go test ./...

race: ## Run all tests with the race detector
	go test -race -count=1 ./...

cover: ## Run tests with coverage and enforce COVER_MIN percent
	go test -race -count=1 -covermode=atomic -coverpkg=./... -coverprofile=coverage.out ./...
	@total=$$(go tool cover -func=coverage.out | awk '/^total:/ {sub("%","",$$3); print $$3}'); \
	echo "total coverage: $$total% (minimum $(COVER_MIN)%)"; \
	awk -v t=$$total -v m=$(COVER_MIN) 'BEGIN { exit (t+0 < m+0) }' || { echo "coverage below minimum"; exit 1; }

vet: ## Run go vet
	go vet ./...

fmt: ## Format code (gofmt + goimports via golangci-lint)
	golangci-lint fmt ./...

lint: ## Run golangci-lint (install with `make tools`)
	golangci-lint run ./...

vuln: ## Scan dependencies and the Go toolchain for known vulnerabilities
	govulncheck ./...

check: lint vet race vuln ## Everything CI checks, except the container build

tools: ## Install pinned developer tools
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)

hooks: ## Enable the repository git hooks
	git config core.hooksPath .githooks
	chmod +x .githooks/*

clean: ## Remove build artifacts and local runtime data (keeps .gitkeep files)
	rm -rf bin/ data/ coverage.out
	find exports inputs -mindepth 1 ! -name .gitkeep -delete 2>/dev/null || true

docker-build: ## Build the container image with version metadata
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_DATE=$(BUILD_DATE) \
		-t $(IMAGE):$(VERSION) -t $(IMAGE):latest .

docker-up: ## Start the stack with Docker Compose
	docker compose up -d --build

docker-down: ## Stop the stack
	docker compose down

docker-smoke: docker-build ## Run the image and push the sample job through it end to end
	./scripts/smoke_test.sh $(IMAGE):$(VERSION)

load-test: ## Run the load test script against a running server
	./scripts/load_test.sh
