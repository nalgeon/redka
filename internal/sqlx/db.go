package sqlx

import (
	"database/sql"
	_ "embed"
	"net/url"
	"runtime"
	"strings"
	"time"
)

// Database schema version.
// const schemaVersion = 1

//go:embed schema.sql
var sqlSchema string

// DefaultPragma is a set of default database settings.
var DefaultPragma = map[string]string{
	"journal_mode": "wal",
	"synchronous":  "normal",
	"temp_store":   "memory",
	"mmap_size":    "268435456",
	"foreign_keys": "on",
}

// Options is the configuration for the database.
type Options struct {
	// Options to set on the database connection.
	Pragma map[string]string
	// Timeout for database operations.
	Timeout time.Duration
	// Whether the database is read-only.
	ReadOnly bool
}

// DB is a database handle.
// Has separate connection pools for read-write and read-only operations.
type DB struct {
	RW      *sql.DB       // read-write handle
	RO      *sql.DB       // read-only handle
	Timeout time.Duration // transaction timeout
}

// Open creates a new database handle.
// Creates the database schema if necessary.
func Open(rw *sql.DB, ro *sql.DB, opts *Options) (*DB, error) {
	d, err := New(rw, ro, opts)
	if err != nil {
		return nil, err
	}
	err = d.createSchema()
	return d, err
}

// newSqlDB creates a new database handle.
// Like openSQL, but does not create the database schema.
func New(rw *sql.DB, ro *sql.DB, opts *Options) (*DB, error) {
	d := &DB{RW: rw, RO: ro, Timeout: opts.Timeout}
	d.setNumConns(opts.ReadOnly)
	err := d.applySettings(opts.Pragma)
	return d, err
}

// setNumConns sets the number of connections.
func (d *DB) setNumConns(readOnly bool) {
	// For the read-only DB handle the number of open connections
	// should be equal to the number of idle connections. Otherwise,
	// the handle will keep opening and closing connections, severely
	// impacting the througput.
	//
	// Benchmarks show that setting nConns>2 does not significantly
	// improve throughput, so I'm not sure what the best value is.
	// For now, I'm setting it to 2-8 depending on the number of CPUs.
	nConns := suggestNumConns()
	d.RO.SetMaxOpenConns(nConns)
	d.RO.SetMaxIdleConns(nConns)

	if !readOnly {
		// SQLite allows only one writer at a time. Setting the maximum
		// number of DB connections to 1 for the read-write DB handle
		// is the best and fastest way to enforce this.
		d.RW.SetMaxOpenConns(1)
	}
}

// applySettings applies the database settings.
func (d *DB) applySettings(pragma map[string]string) error {
	// Ideally, we'd only set the pragmas in the connection string
	// (see [DataSource]), so we wouldn't need this function.
	// But since the mattn driver does not support setting pragmas
	// in the connection string, we also set them here.
	//
	// The correct way to set pragmas for the mattn driver is to
	// use the connection hook (see cmd/redka/main.go on how to do this).
	// But since we can't be sure the user does that, we also set them here.
	//
	// Unfortunately, setting pragmas using Exec only sets them for
	// a single connection. It's not a problem for d.RW (which has only
	// one connection), but it is for d.RO (which has multiple connections).
	// Still, it's better than nothing.
	//
	// See https://github.com/nalgeon/redka/issues/28 for more details.
	if len(pragma) == 0 {
		return nil
	}

	var query strings.Builder
	for name, val := range pragma {
		query.WriteString("pragma ")
		query.WriteString(name)
		query.WriteString("=")
		query.WriteString(val)
		query.WriteString(";")
	}
	if _, err := d.RW.Exec(query.String()); err != nil {
		return err
	}
	if _, err := d.RO.Exec(query.String()); err != nil {
		return err
	}
	return nil
}

// createSchema creates the database schema.
func (d *DB) createSchema() error {
	_, err := d.RW.Exec(sqlSchema)
	return err
}

// DataSource returns an SQLite connection string
// for a read-only or read-write mode.
func DataSource(path string, readOnly bool, pragma map[string]string) string {
	var ds string

	// Parse the parameters.
	source, query, _ := strings.Cut(path, "?")
	params, _ := url.ParseQuery(query)

	if source == ":memory:" {
		// This is an in-memory database, it must have a shared cache.
		// https://www.sqlite.org/sharedcache.html#shared_cache_and_in_memory_databases
		ds = "file:redka"
		params.Set("mode", "memory")
		params.Set("cache", "shared")
	} else {
		// This is a file-based database, it must have a "file:" prefix
		// for setting parameters (https://www.sqlite.org/c3ref/open.html).
		ds = source
		if !strings.HasPrefix(ds, "file:") {
			ds = "file:" + ds
		}
	}

	// sql.DB is concurrent-safe, so we don't need SQLite mutexes.
	params.Set("_mutex", "no")

	// Set the connection mode (writable or read-only).
	if readOnly {
		if params.Get("mode") != "memory" {
			// Enable read-only mode for read-only databases
			// (except for in-memory databases, which are always writable).
			// https://www.sqlite.org/c3ref/open.html
			params.Set("mode", "ro")
		}
	} else {
		// Enable IMMEDIATE transactions for writable databases.
		// https://www.sqlite.org/lang_transaction.html
		params.Set("_txlock", "immediate")
	}

	// Apply the pragma settings.
	// Some drivers (modernc and ncruces) setting passing pragmas
	// in the connection string, so we add them here.
	// The mattn driver does not support this, so it'll just ignore them.
	// For mattn driver, we have to set the pragmas in the connection hook.
	// (see cmd/redka/main.go on how to do this).
	for name, val := range pragma {
		params.Add("_pragma", name+"="+val)
	}

	return ds + "?" + params.Encode()
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
