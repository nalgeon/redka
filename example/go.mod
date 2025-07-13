module github.com/nalgeon/redka/example

replace github.com/nalgeon/redka => ../

go 1.23.0

toolchain go1.24.0

require (
	github.com/lib/pq v1.10.9
	github.com/mattn/go-sqlite3 v1.14.28
	github.com/nalgeon/redka v0.0.0-00010101000000-000000000000
	github.com/ncruces/go-sqlite3 v0.16.2
	github.com/redis/go-redis/v9 v9.11.0
	modernc.org/sqlite v1.29.5
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/ncruces/julianday v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/tetratelabs/wazero v1.7.3 // indirect
	github.com/tidwall/btree v1.7.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/redcon v1.6.2 // indirect
	golang.org/x/sys v0.21.0 // indirect
	golang.org/x/tools v0.19.0 // indirect
	modernc.org/gc/v3 v3.0.0-20240107210532-573471604cb6 // indirect
	modernc.org/libc v1.41.0 // indirect
	modernc.org/mathutil v1.6.0 // indirect
	modernc.org/memory v1.7.2 // indirect
	modernc.org/strutil v1.2.0 // indirect
	modernc.org/token v1.1.0 // indirect
)
