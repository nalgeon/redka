module github.com/nalgeon/redka

go 1.23.0

toolchain go1.24.0

// Main dependencies.
require github.com/tidwall/redcon v1.6.2

// Test dependencies.
require (
	github.com/lib/pq v1.10.9
	github.com/mattn/go-sqlite3 v1.14.28
	github.com/nalgeon/be v0.1.0
)

require (
	github.com/tidwall/btree v1.7.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
)
