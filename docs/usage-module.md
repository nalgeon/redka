# Using Redka as a Go module

If you are using Go, Redka is available as a module without the need to start a separate server.

See the [package documentation](https://pkg.go.dev/github.com/nalgeon/redka) for API reference.

## Opening the database

The primary object in Redka is the `DB`. To open or create your database, use the `redka.Open()` function:

```go
package main

import (
    "log"

    _ "github.com/mattn/go-sqlite3"
    "github.com/nalgeon/redka"
)

func main() {
    // Open or create the redka.db file.
    db, err := redka.Open("redka.db", nil)
    if err != nil {
        log.Fatal(err)
    }
    // Always close the database when you are finished.
    defer db.Close()
    // ...
}
```

Don't forget to import the driver (here I use `github.com/mattn/go-sqlite3`). See the list of the supported drivers below.

To open an in-memory database that doesn't persist to disk, use the following path:

```go
// All data is lost when the database is closed.
redka.Open("file:/redka.db?vfs=memdb", nil)
```

## Running commands

After opening the database, call `redka.DB` methods to run individual commands:

```go
db.Str().Set("name", "alice")
db.Str().Set("age", 25)

name, err := db.Str().Get("name")
slog.Info("get", "name", name, "err", err)

age, err := db.Str().Get("age")
slog.Info("get", "age", age, "err", err)
```

```text
get name="alice" err=<nil>
get age="25" err=<nil>
```

See the full example in [example/mattn/main.go](../example/mattn/main.go).

## Transactions

Use transactions to batch commands. There are `View` (read-only transaction) and `Update` (writable transaction) methods for this:

```go
updCount := 0
err := db.Update(func(tx *redka.Tx) error {
    err := tx.Str().Set("name", "bob")
    if err != nil {
        return err
    }
    updCount++

    err = tx.Str().Set("age", 50)
    if err != nil {
        return err
    }
    updCount++
    return nil
})
slog.Info("updated", "count", updCount, "err", err)
```

```text
updated count=2 err=<nil>
```

See the full example in [example/tx/main.go](../example/tx/main.go).

## Supported drivers

Redka supports the following SQLite drivers:

-   `github.com/mattn/go-sqlite3` ([example](../example/mattn/main.go))
-   `github.com/ncruces/go-sqlite3` ([example](../example/ncruces/main.go))
-   `modernc.org/sqlite` ([example](../example/modernc/main.go))

And the following Postgres drivers:

-   `github.com/lib/pq` ([example](../example/postgres/main.go))
-   `github.com/jackc/pgx/v5`
