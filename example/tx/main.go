// An example of using Redka with transactions.
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

	{
		// Writable transaction.
		updCount := 0
		err := db.Update(func(tx *redka.Tx) error {
			err := tx.Str().Set("name", "alice")
			if err != nil {
				return err
			}
			updCount++

			err = tx.Str().Set("age", 25)
			if err != nil {
				return err
			}
			updCount++

			return nil
		})
		slog.Info("updated", "count", updCount, "err", err)
	}

	{
		// Read-only transaction.
		type person struct {
			name string
			age  int
		}

		var p person
		err := db.View(func(tx *redka.Tx) error {
			name, err := tx.Str().Get("name")
			if err != nil {
				return err
			}
			p.name = name.String()

			age, err := tx.Str().Get("age")
			if err != nil {
				return err
			}
			// Only use MustInt() if you are sure that
			// the key exists and is an integer.
			p.age = age.MustInt()
			return nil
		})
		slog.Info("get", "person", p, "err", err)
	}
}
