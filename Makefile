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
	@echo "> running tests with $(driver) driver..."
	@go test -tags=$(driver) ./...
	@echo "✓ finished tests"

test-sqlite:
	@echo "> running tests with sqlite driver..."
	@go test -tags=sqlite3 ./...
	@echo "✓ finished tests"

test-postgres:
	@echo "> running tests with postgres driver..."
	@go test -tags=postgres -p=1 ./...
	@echo "✓ finished tests"

build:
	@CGO_ENABLED=1 go build -ldflags "-s -w -X main.version=$(build_ver) -X main.commit=$(build_rev) -X main.date=$(build_date)" -trimpath -o build/redka -v cmd/redka/main.go

build-cli:
	@CGO_ENABLED=1 go build -ldflags "-s -w" -trimpath -o build/redka-cli -v cmd/cli/main.go

run:
	@./build/redka

postgres-start:
	@echo "> starting postgres..."
	@docker run --rm --detach --name=redka-postgres \
		--env=POSTGRES_USER=redka \
		--env=POSTGRES_PASSWORD=redka \
		--env=POSTGRES_DB=redka \
		--publish=5432:5432 \
		postgres:17-alpine
	@until docker exec redka-postgres \
		pg_isready --username=redka --dbname=redka --quiet --quiet; \
		do sleep 1; done
	@echo "✓ started postgres"

postgres-stop:
	@echo "> stopping postgres..."
	@docker stop redka-postgres
	@echo "✓ stopped postgres"

postgres-shell:
	@docker exec -it redka-postgres psql --username=redka --dbname=redka