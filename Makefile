.PHONY: setup lint vet test build run

ifneq ($(wildcard .git),)
	build_rev := $(shell git rev-parse --short HEAD)
endif
build_date := $(shell date -u '+%Y-%m-%dT%H:%M:%S')

setup:
	@go mod download

lint:
	@golangci-lint run --print-issued-lines=false --out-format=colored-line-number ./...

vet:
	@go vet ./...

test:
	@go test ./... -v


build:
	@CGO_ENABLED=1 go build -ldflags "-s -w -X main.commit=$(build_rev) -X main.date=$(build_date)" -trimpath -o build/redka -v cmd/redka/main.go

run:
	@./build/redka
