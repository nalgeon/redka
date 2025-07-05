package testx

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nalgeon/redka"
)

// driver to connect to the test database.
var driver string

// connStrings are connection strings for test databases.
var connStrings = map[string]string{
	"postgres": "postgres://redka:redka@localhost:5432/redka?sslmode=disable",
	"sqlite3":  "file:/redka.db?vfs=memdb",
}

// OpenDB returns a database handle for testing.
// Uses the driver specified in the build tag.
func OpenDB(tb testing.TB) *redka.DB {
	tb.Helper()

	// Get the database connection string.
	connStr := connStrings[driver]
	if connStr == "" {
		tb.Fatalf("unknown driver: %s", driver)
	}

	// Open the database.
	sdb, err := sql.Open(driver, connStr)
	if err != nil {
		tb.Fatal(err)
	}
	db, err := redka.OpenDB(sdb, sdb, &redka.Options{DriverName: driver})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		db.Close()
	})

	// Clear all tables.
	_, err = sdb.Exec("delete from rkey")
	if err != nil {
		tb.Fatal(err)
	}

	return db
}
