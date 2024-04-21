// Package Redka implements Redis-like database backed by SQLite.
// It provides an API to interact with data structures like keys,
// strings and hashes.
//
// Typically, you open a database with [Open] and use the returned
// [DB] instance methods like [DB.Key] or [DB.Str] to access the
// data structures. You should only use one instance of DB throughout
// your program and close it with [DB.Close] when the program exits.
package redka

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rhash"
	"github.com/nalgeon/redka/internal/rkey"
	"github.com/nalgeon/redka/internal/rstring"
	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/sqlx"
)

const driverName = "sqlite3"

// Common errors returned by data structure methods.
var (
	ErrNotFound  = core.ErrNotFound  // key not found
	ErrKeyType   = core.ErrKeyType   // key type mismatch
	ErrValueType = core.ErrValueType // invalid value type
)

// Key represents a key data structure.
// Each key uniquely identifies a data structure stored in the
// database (e.g. a string, a list, or a hash). There can be only one
// data structure with a given key, regardless of type. For example,
// you can't have a string and a hash map with the same key.
type Key = core.Key

// Value represents a value stored in a database (a byte slice).
// It can be converted to other scalar types.
type Value = core.Value

// Options is the configuration for the database.
type Options struct {
	// Logger is the logger for the database.
	// If nil, a silent logger is used.
	Logger *slog.Logger
}

var defaultOptions = Options{
	Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
}

// DB is a Redis-like database backed by SQLite.
// Provides access to data structures like keys, strings, and hashes.
//
// DB is safe for concurrent use by multiple goroutines as long as you use
// a single instance of DB throughout your program.
type DB struct {
	*sqlx.DB[*Tx]
	keyDB    *rkey.DB
	stringDB *rstring.DB
	hashDB   *rhash.DB
	zsetDB   *rzset.DB
	bg       *time.Ticker
	log      *slog.Logger
}

// Open opens a new or existing database at the given path.
// Creates the database schema if necessary.
//
// Expects the database driver to be already imported with the name "sqlite3".
// See the [simple] and [modernc] examples for details.
//
// The returned [DB] is safe for concurrent use by multiple goroutines
// as long as you use a single instance throughout your program.
// Typically, you only close the DB when the program exits.
//
// The opts parameter is optional. If nil, uses default options.
//
// [simple]: https://github.com/nalgeon/redka/blob/main/example/simple/main.go
// [modernc]: https://github.com/nalgeon/redka/blob/main/example/modernc/main.go
func Open(path string, opts *Options) (*DB, error) {
	db, err := sql.Open(driverName, path)
	if err != nil {
		return nil, err
	}
	sdb, err := sqlx.Open(db, newTx)
	if err != nil {
		return nil, err
	}
	opts = applyOptions(defaultOptions, opts)
	rdb := &DB{
		DB:       sdb,
		keyDB:    rkey.New(db),
		stringDB: rstring.New(db),
		hashDB:   rhash.New(db),
		zsetDB:   rzset.New(db),
		log:      opts.Logger,
	}
	rdb.bg = rdb.startBgManager()
	return rdb, nil
}

// Str returns the string repository.
// A string is a slice of bytes associated with a key.
// Use the string repository to work with individual strings.
func (db *DB) Str() *rstring.DB {
	return db.stringDB
}

// Hash returns the hash repository.
// A hash (hashmap) is a field-value map associated with a key.
// Use the hash repository to work with individual hashmaps
// and their fields.
func (db *DB) Hash() *rhash.DB {
	return db.hashDB
}

// SortedSet returns the sorted set repository.
// A sorted set (zset) is a like a set, but each element has a score,
// and elements are ordered by score from low to high.
// Use the sorted set repository to work with individual sets
// and their elements, and to perform set operations.
func (db *DB) SortedSet() *rzset.DB {
	return db.zsetDB
}

// Key returns the key repository.
// A key is a unique identifier for a data structure
// (string, list, hash, etc.). Use the key repository
// to manage all keys regardless of their type.
func (db *DB) Key() *rkey.DB {
	return db.keyDB
}

// Update executes a function within a writable transaction.
// See the [tx] example for details.
//
// [tx]: https://github.com/nalgeon/redka/blob/main/example/tx/main.go
func (db *DB) Update(f func(tx *Tx) error) error {
	return db.DB.Update(f)
}

// UpdateContext executes a function within a writable transaction.
// See the [tx] example for details.
//
// [tx]: https://github.com/nalgeon/redka/blob/main/example/tx/main.go
func (db *DB) UpdateContext(ctx context.Context, f func(tx *Tx) error) error {
	return db.DB.UpdateContext(ctx, f)
}

// View executes a function within a read-only transaction.
// See the [tx] example for details.
//
// [tx]: https://github.com/nalgeon/redka/blob/main/example/tx/main.go
func (db *DB) View(f func(tx *Tx) error) error {
	return db.DB.View(f)
}

// ViewContext executes a function within a read-only transaction.
// See the [tx] example for details.
//
// [tx]: https://github.com/nalgeon/redka/blob/main/example/tx/main.go
func (db *DB) ViewContext(ctx context.Context, f func(tx *Tx) error) error {
	return db.DB.ViewContext(ctx, f)
}

// Close closes the database.
// It's safe for concurrent use by multiple goroutines.
func (db *DB) Close() error {
	db.bg.Stop()
	return db.SQL.Close()
}

// startBgManager starts the goroutine than runs
// in the background and deletes expired keys.
// Triggers every 60 seconds, deletes up all expired keys.
func (db *DB) startBgManager() *time.Ticker {
	// TODO: needs further investigation. Deleting all keys may be expensive
	// and lead to timeouts for concurrent write operations.
	// Adaptive limits based on the number of changed keys may be a solution.
	// (see https://redis.io/docs/management/config-file/ > SNAPSHOTTING)
	// And it doesn't help that SQLite's drivers do not support DELETE LIMIT,
	// so we have to use DELETE IN (SELECT ...), which is more expensive.
	const interval = 60 * time.Second
	const nKeys = 0

	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			count, err := db.keyDB.DeleteExpired(nKeys)
			if err != nil {
				db.log.Error("bg: delete expired keys", "error", err)
			} else {
				db.log.Info("bg: delete expired keys", "count", count)
			}
		}
	}()
	return ticker
}

// Tx is a Redis-like database transaction.
// Same as [DB], Tx provides access to data structures like keys,
// strings, and hashes. The difference is that you call Tx methods
// within a transaction managed by [DB.Update] or [DB.View].
//
// See the [tx] example for details.
//
// [tx]: https://github.com/nalgeon/redka/blob/main/example/tx/main.go
type Tx struct {
	tx     sqlx.Tx
	keyTx  *rkey.Tx
	strTx  *rstring.Tx
	hashTx *rhash.Tx
	zsetTx *rzset.Tx
}

// newTx creates a new database transaction.
func newTx(tx sqlx.Tx) *Tx {
	return &Tx{tx: tx,
		keyTx:  rkey.NewTx(tx),
		strTx:  rstring.NewTx(tx),
		hashTx: rhash.NewTx(tx),
		zsetTx: rzset.NewTx(tx),
	}
}

// Str returns the string transaction.
func (tx *Tx) Str() *rstring.Tx {
	return tx.strTx
}

// Keys returns the key transaction.
func (tx *Tx) Key() *rkey.Tx {
	return tx.keyTx
}

// Hash returns the hash transaction.
func (tx *Tx) Hash() *rhash.Tx {
	return tx.hashTx
}

// SortedSet returns the sorted set transaction.
func (tx *Tx) SortedSet() *rzset.Tx {
	return tx.zsetTx
}

// applyOptions applies custom options to the
// default options and returns the result.
func applyOptions(opts Options, custom *Options) *Options {
	if custom == nil {
		return &opts
	}
	if custom.Logger != nil {
		opts.Logger = custom.Logger
	}
	return &opts
}
