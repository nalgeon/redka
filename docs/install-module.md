# Installing Redka as a Go module

Install the module as follows:

```shell
go get github.com/nalgeon/redka
```

You'll also need an SQLite or PostgreSQL driver.

Use one of the following for SQLite:

-   `github.com/mattn/go-sqlite3` (CGO, fastest)
-   `github.com/ncruces/go-sqlite3` (pure Go, WASM)
-   `modernc.org/sqlite` (pure Go, libc port)

Or one of the following for PostgreSQL:

-   `github.com/lib/pq`
-   `github.com/jackc/pgx/v5`

Install a driver with `go get` like this:

```shell
go get github.com/mattn/go-sqlite3
```
