module github.com/nalgeon/redka

go 1.23.0

// Main dependencies.
require github.com/tidwall/redcon v1.6.2

// Test dependencies.
require (
	github.com/lib/pq v1.11.1
	github.com/mattn/go-sqlite3 v1.14.33
	github.com/nalgeon/be v0.3.0
)

require (
	github.com/tidwall/btree v1.7.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
)
