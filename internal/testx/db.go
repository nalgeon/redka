package testx

import (
	"database/sql"
	"testing"

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
	return OpenDBOpts(tb, nil)
}

// OpenDBOpts returns a database handle for testing.
// Uses the driver specified in the build tag.
func OpenDBOpts(tb testing.TB, opts *redka.Options) *redka.DB {
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
	if opts == nil {
		opts = &redka.Options{DriverName: driver}
	} else {
		opts.DriverName = driver
	}
	db, err := redka.OpenDB(sdb, sdb, opts)
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
