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
	db, err := redka.Open("data.db", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Str().Set("name", "alice")
	slog.Info("set", "err", err)

	count, err := db.Key().Count("name", "age", "city")
	slog.Info("count", "count", count, "err", err)
}
