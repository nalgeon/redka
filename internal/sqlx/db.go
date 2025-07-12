package sqlx

import (
	"database/sql"
	"runtime"
	"time"
)

// Database schema version.
// const schemaVersion = 1

// Options is the configuration for the database.
type Options struct {
	// SQL dialect.
	Dialect Dialect
	// Options to set on the database connection.
	// If nil, uses the engine-specific defaults.
	// If the map is empty, no options are set.
	Pragma map[string]string
	// Timeout for database operations.
	Timeout time.Duration
	// Whether the database is read-only.
	ReadOnly bool
}

// DB is a database handle.
// Has separate connection pools for read-write and read-only operations.
type DB struct {
	Dialect Dialect       // database dialect
	RW      *sql.DB       // read-write handle
	RO      *sql.DB       // read-only handle
	Timeout time.Duration // transaction timeout
}

// Open creates a new database handle.
// Creates the database schema if necessary.
func Open(rw *sql.DB, ro *sql.DB, opts *Options) (*DB, error) {
	switch opts.Dialect {
	case DialectSqlite:
		d, err := openSqlite(rw, ro, opts)
		return (*DB)(d), err
	case DialectPostgres:
		d, err := openPostgres(rw, ro, opts)
		return (*DB)(d), err
	default:
		return nil, ErrDialect
	}
}

// New creates a new database handle.
// Like Open, but does not create the database schema.
func New(rw *sql.DB, ro *sql.DB, opts *Options) (*DB, error) {
	switch opts.Dialect {
	case DialectSqlite:
		d, err := newSqlite(rw, ro, opts)
		return (*DB)(d), err
	case DialectPostgres:
		d, err := newPostgres(rw, ro, opts)
		return (*DB)(d), err
	default:
		return nil, ErrDialect
	}
}

// DataSource returns a connection string
// for a read-only or read-write mode.
func DataSource(path string, readOnly bool, opts *Options) string {
	switch opts.Dialect {
	case DialectSqlite:
		return sqliteDataSource(path, readOnly, opts.Pragma)
	case DialectPostgres:
		return postgresDataSource(path, readOnly, opts.Pragma)
	default:
		return ""
	}
}

// suggestNumConns calculates the optimal number
// of parallel connections to the database.
func suggestNumConns() int {
	ncpu := runtime.NumCPU()
	switch {
	case ncpu < 2:
		return 2
	case ncpu > 8:
		return 8
	default:
		return ncpu
	}
}
