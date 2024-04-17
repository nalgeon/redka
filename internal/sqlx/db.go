package sqlx

import (
	"context"
	"database/sql"
	_ "embed"
	"sync"
)

// Database schema version.
// const schemaVersion = 1

// Default SQL settings.
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
type DB[T any] struct {
	SQL *sql.DB
	// newT creates a new domain-specific transaction.
	newT func(Tx) T
	sync.Mutex
}

// Open creates a new database-backed repository.
// Creates the database schema if necessary.
func Open[T any](db *sql.DB, newT func(Tx) T) (*DB[T], error) {
	d := New(db, newT)
	err := d.init()
	return d, err
}

// newSqlDB creates a new database-backed repository.
// Like openSQL, but does not create the database schema.
func New[T any](db *sql.DB, newT func(Tx) T) *DB[T] {
	d := &DB[T]{SQL: db, newT: newT}
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
	// SQLite only allows one writer at a time, so concurrent writes
	// will fail with a "database is locked" (SQLITE_BUSY) error.
	//
	// There are two ways to enforce the single writer rule:
	// 1. Use a mutex for write operations (in the execTx method).
	// 2. Set the maximum number of DB connections to 1.
	//
	// Intuitively, the mutex approach seems better, because it does not
	// limit the number of concurrent read operations. The benchmarks
	// show the following results:
	// - GET: 2% better rps and 25% better p50 response time with mutex
	// - SET: 2% better rps and 60% worse p50 response time with mutex
	//
	// Due to the significant p50 response time mutex penalty for SET,
	// I've decided to use the max connections approach for now.
	d.SQL.SetMaxOpenConns(1)
	if _, err := d.SQL.Exec(sqlSettings); err != nil {
		return err
	}
	if _, err := d.SQL.Exec(sqlSchema); err != nil {
		return err
	}
	return nil
}

// execTx executes a function within a transaction.
func (d *DB[T]) execTx(ctx context.Context, writable bool, f func(tx T) error) error {
	// See the init method for the explanation of the single writer rule.
	// if writable {
	// 	// only one writable transaction at a time
	// 	d.Lock()
	// 	defer d.Unlock()
	// }

	dtx, err := d.SQL.BeginTx(ctx, nil)
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
