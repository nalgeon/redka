package main

import (
	"log"
	"log/slog"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nalgeon/redka"
)

func main() {
	// A simple example of using Redka.

	// Open a database.
	db, err := redka.Open("data.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Set some string keys.
	err = db.Str().Set("name", "alice")
	slog.Info("set", "err", err)
	err = db.Str().Set("age", 25)
	slog.Info("set", "err", err)

	// Check if the keys exist.
	count, err := db.Key().Count("name", "age", "city")
	slog.Info("count", "count", count, "err", err)

	// Get a key.
	name, err := db.Str().Get("name")
	slog.Info("get", "name", name, "err", err)
}
