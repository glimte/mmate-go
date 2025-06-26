.PHONY: build test lint clean install-tools fmt vet mod-tidy run-tests integration-tests

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=gofmt
GOVET=$(GOCMD) vet

# Project parameters
MONITOR_BINARY=mmate-monitor
MONITOR_PATH=./cmd/monitor
PACKAGES=./...

# Default target
all: test build

# Build the project
build:
	@echo "Building..."
	$(GOBUILD) -v $(PACKAGES)
	$(GOBUILD) -o $(MONITOR_BINARY) $(MONITOR_PATH)

# Run tests
test:
	@echo "Running tests..."
	$(GOTEST) -v -race -coverprofile=coverage.out $(PACKAGES)

# Run tests with coverage report
test-coverage: test
	@echo "Generating coverage report..."
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run integration tests
integration-tests:
	@echo "Running integration tests..."
	$(GOTEST) -v -race -tags=integration $(PACKAGES)

# Run linting
lint: install-tools
	@echo "Running linter..."
	golangci-lint run ./...

# Format code
fmt:
	@echo "Formatting code..."
	$(GOFMT) -s -w .

# Run go vet
vet:
	@echo "Running go vet..."
	$(GOVET) $(PACKAGES)

# Tidy modules
mod-tidy:
	@echo "Tidying modules..."
	$(GOMOD) tidy

# Install development tools
install-tools:
	@echo "Installing development tools..."
	@which golangci-lint > /dev/null || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.55.2

# Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -f $(MONITOR_BINARY)
	rm -f coverage.out
	rm -f coverage.html

# Run all checks
check: fmt vet lint test

# Quick development cycle
dev: fmt vet test

# Install dependencies
deps:
	@echo "Installing dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy

# Run the monitor tool
run-monitor: build
	./$(MONITOR_BINARY)

# Help
help:
	@echo "Available targets:"
	@echo "  build          - Build the project"
	@echo "  test           - Run tests"
	@echo "  test-coverage  - Run tests with coverage report"
	@echo "  integration-tests - Run integration tests"
	@echo "  lint           - Run linter"
	@echo "  fmt            - Format code"
	@echo "  vet            - Run go vet"
	@echo "  mod-tidy       - Tidy modules"
	@echo "  install-tools  - Install development tools"
	@echo "  clean          - Clean build artifacts"
	@echo "  check          - Run all checks (fmt, vet, lint, test)"
	@echo "  dev            - Quick development cycle (fmt, vet, test)"
	@echo "  deps           - Install dependencies"
	@echo "  run-monitor    - Run the monitor tool"
	@echo "  help           - Show this help message"