.PHONY: setup lint vet test build run

has_git := $(shell command -v git 2>/dev/null)

ifdef has_git
build_rev := $(shell git rev-parse --short HEAD)
git_tag := $(shell git describe --tags --exact-match 2>/dev/null)
else
build_rev := unknown
endif

ifdef git_tag
build_ver := $(git_tag)
else
build_ver := main
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
	@CGO_ENABLED=1 go build -ldflags "-s -w -X main.version=$(build_ver) -X main.commit=$(build_rev) -X main.date=$(build_date)" -trimpath -o build/redka -v cmd/redka/main.go

build-cli:
	@CGO_ENABLED=1 go build -ldflags "-s -w" -trimpath -o build/redka-cli -v cmd/cli/main.go

run:
	@./build/redka
