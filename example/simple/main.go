// A basic example of using Redka
// with github.com/mattn/go-sqlite3 driver.
package main

import (
	"log"
	"log/slog"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nalgeon/redka"
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
