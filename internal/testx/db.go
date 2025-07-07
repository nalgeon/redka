package testx

import (
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
	if opts == nil {
		opts = &redka.Options{DriverName: driver}
	} else {
		opts.DriverName = driver
	}
	db, err := redka.Open(connStr, opts)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		db.Close()
	})

	// Clear the database.
	err = db.Key().DeleteAll()
	if err != nil {
		tb.Fatal(err)
	}

	return db
}
