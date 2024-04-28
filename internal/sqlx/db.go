package sqlx

import (
	"context"
	"database/sql"
	_ "embed"
	"net/url"
	"runtime"
	"strings"
	"sync"
)

// Database schema version.
// const schemaVersion = 1

// Default database settings.
const sqlSettings = `
pragma journal_mode = wal;
pragma synchronous = normal;
pragma temp_store = memory;
pragma mmap_size = 268435456;
pragma foreign_keys = on;
`

//go:embed schema.sql
var sqlSchema string

// DB is a generic database-backed repository
// with a domain-specific transaction of type T.
// Has separate database handles for read-write
// and read-only operations.
type DB[T any] struct {
	RW   *sql.DB    // read-write handle
	RO   *sql.DB    // read-only handle
	newT func(Tx) T // creates a new domain transaction
	sync.Mutex
}

// Open creates a new database-backed repository.
// Creates the database schema if necessary.
func Open[T any](rw *sql.DB, ro *sql.DB, newT func(Tx) T) (*DB[T], error) {
	d := New(rw, ro, newT)
	err := d.init()
	return d, err
}

// newSqlDB creates a new database-backed repository.
// Like openSQL, but does not create the database schema.
func New[T any](rw *sql.DB, ro *sql.DB, newT func(Tx) T) *DB[T] {
	d := &DB[T]{RW: rw, RO: ro, newT: newT}
	return d
}

// Update executes a function within a writable transaction.
func (d *DB[T]) Update(f func(tx T) error) error {
	return d.UpdateContext(context.Background(), f)
}

// UpdateContext executes a function within a writable transaction.
func (d *DB[T]) UpdateContext(ctx context.Context, f func(tx T) error) error {
	return d.execTx(ctx, true, f)
}

// View executes a function within a read-only transaction.
func (d *DB[T]) View(f func(tx T) error) error {
	return d.ViewContext(context.Background(), f)
}

// ViewContext executes a function within a read-only transaction.
func (d *DB[T]) ViewContext(ctx context.Context, f func(tx T) error) error {
	return d.execTx(ctx, false, f)
}

// Init sets the connection properties and creates the necessary tables.
func (d *DB[T]) init() error {
	// SQLite allows only one writer at a time. Setting the maximum
	// number of DB connections to 1 for the read-write DB handle
	// is the best and fastest way to enforce this.
	d.RW.SetMaxOpenConns(1)

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

	// Set the journal mode and other settings.
	if _, err := d.RW.Exec(sqlSettings); err != nil {
		return err
	}
	if _, err := d.RO.Exec(sqlSettings); err != nil {
		return err
	}
	// Create the schema.
	if _, err := d.RW.Exec(sqlSchema); err != nil {
		return err
	}
	return nil
}

// execTx executes a function within a transaction.
func (d *DB[T]) execTx(ctx context.Context, writable bool, f func(tx T) error) error {
	var dtx *sql.Tx
	var err error
	if writable {
		dtx, err = d.RW.BeginTx(ctx, nil)
	} else {
		dtx, err = d.RO.BeginTx(ctx, nil)
	}

	if err != nil {
		return err
	}
	defer func() { _ = dtx.Rollback() }()

	tx := d.newT(dtx)
	err = f(tx)
	if err != nil {
		return err
	}
	return dtx.Commit()
}

// DataSource returns an SQLite connection string
// for a read-only or read-write mode.
func DataSource(path string, writable bool) string {
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

	if writable {
		// Enable IMMEDIATE transactions for writable databases.
		// https://www.sqlite.org/lang_transaction.html
		params.Set("_txlock", "immediate")
	} else if params.Get("mode") != "memory" {
		// Enable read-only mode for read-only databases
		// (except for in-memory databases, which are always writable).
		// https://www.sqlite.org/c3ref/open.html
		params.Set("mode", "ro")
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
