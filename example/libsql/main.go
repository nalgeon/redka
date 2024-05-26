// An example of using Redka
// with github.com/tursodatabase/go-libsql driver.
package main

import (
	"log"
	"log/slog"

	"github.com/nalgeon/redka"
	_ "github.com/tursodatabase/go-libsql"
)

func main() {
	// libSQL uses a different driver name ("libsql" instead of "sqlite3").
	// It also does not support the journal_mode and mmap_size pragmas
	// (see https://github.com/tursodatabase/go-libsql/issues/28),
	// so we have to turn them off.
	opts := redka.Options{
		DriverName: "libsql",
		Pragma: map[string]string{
			"synchronous":  "normal",
			"temp_store":   "memory",
			"foreign_keys": "on",
		},
	}
	db, err := redka.Open("data.db", &opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Str().Set("name", "alice")
	slog.Info("set", "err", err)

	count, err := db.Key().Count("name", "age", "city")
	slog.Info("count", "count", count, "err", err)
}
