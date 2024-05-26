// An example of using Redka
// with modernc.org/sqlite driver.
package main

import (
	"log"
	"log/slog"

	"github.com/nalgeon/redka"
	_ "modernc.org/sqlite"
)

func main() {
	// modernc.org/sqlite uses a different driver name
	// ("sqlite" instead of "sqlite3").
	opts := redka.Options{
		DriverName: "sqlite",
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
