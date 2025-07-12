// An example of using Redka
// with github.com/lib/pq driver.
package main

import (
	"log"
	"log/slog"

	_ "github.com/lib/pq"
	"github.com/nalgeon/redka"
)

func main() {
	// Connections settings.
	connString := "postgres://redka:redka@localhost:5432/redka?sslmode=disable"
	opts := &redka.Options{DriverName: "postgres"}

	// Open the database.
	db, err := redka.Open(connString, opts)
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
