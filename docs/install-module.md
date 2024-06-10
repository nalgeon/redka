# Installing Redka as a Go module

Install the module as follows:

```shell
go get github.com/nalgeon/redka
```

You'll also need an SQLite driver. Use one of the following:

-   `github.com/mattn/go-sqlite3` (CGO, fastest)
-   `github.com/ncruces/go-sqlite3` (pure Go, WASM)
-   `github.com/tursodatabase/go-libsql` (CGO)
-   `modernc.org/sqlite` (pure Go)

Install a driver with `go get` like this:

```shell
go get github.com/mattn/go-sqlite3
```
