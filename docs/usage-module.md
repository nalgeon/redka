# Using Redka as a Go module

The primary object in Redka is the `DB`. To open or create your database, use the `redka.Open()` function:

```go
package main

import (
    "log"

    _ "github.com/mattn/go-sqlite3"
    "github.com/nalgeon/redka"
)

func main() {
    // Open or create the data.db file.
    db, err := redka.Open("data.db", nil)
    if err != nil {
        log.Fatal(err)
    }
    // Always close the database when you are finished.
    defer db.Close()
    // ...
}
```

Don't forget to import the driver (here I use `github.com/mattn/go-sqlite3`). Using `modernc.org/sqlite` is slightly different, see [example/modernc/main.go](example/modernc/main.go) for details.

To open an in-memory database that doesn't persist to disk, use the following path:

```go
// All data is lost when the database is closed.
redka.Open("file:/data.db?vfs=memdb")
```

After opening the database, call `redka.DB` methods to run individual commands:

```go
db.Str().Set("name", "alice")
db.Str().Set("age", 25)

count, err := db.Key().Count("name", "age", "city")
slog.Info("count", "count", count, "err", err)

name, err := db.Str().Get("name")
slog.Info("get", "name", name, "err", err)
```

```
count count=2 err=<nil>
get name="alice" err=<nil>
```

See the full example in [example/simple/main.go](example/simple/main.go).

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

```
updated count=2 err=<nil>
```

See the full example in [example/tx/main.go](example/tx/main.go).

See the [package documentation](https://pkg.go.dev/github.com/nalgeon/redka) for API reference.
