// An example of using Redka
// with github.com/ncruces/go-sqlite3 driver.
package main

import (
	"log"
	"log/slog"

	"github.com/nalgeon/redka"
	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

func main() {
	// Open the database.
	db, err := redka.Open("redka.db", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Set some values.
	err = db.Str().Set("name", "alice")
	slog.Info("set", "err", err)
	err = db.Str().Set("age", 25)
	slog.Info("set", "err", err)

	// Read them back.
	name, err := db.Str().Get("name")
	slog.Info("get", "name", name, "err", err)
	age, err := db.Str().Get("age")
	slog.Info("get", "age", age, "err", err)
}
