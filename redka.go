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
	"github.com/nalgeon/redka/internal/rlist"
	"github.com/nalgeon/redka/internal/rset"
	"github.com/nalgeon/redka/internal/rstring"
	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/sqlx"
)

// Common errors returned by data structure methods.
var (
	ErrKeyType   = core.ErrKeyType   // key type mismatch
	ErrNotFound  = core.ErrNotFound  // key or element not found
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
	// SQL driver name.
	// If empty, uses "sqlite3".
	DriverName string
	// SQL pragmas to set on the database connection.
	// If nil, uses the default pragmas:
	//  - journal_mode=wal
	//  - synchronous=normal
	//  - temp_store=memory
	//  - mmap_size=268435456
	//  - foreign_keys=on
	Pragma map[string]string
	// Logger for the database. If nil, uses a silent logger.
	Logger *slog.Logger

	// If true, opens the database in read-only mode.
	readonly bool
}

var defaultOptions = Options{
	DriverName: "sqlite3",
	Pragma:     sqlx.DefaultPragma,
	Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
}

// DB is a Redis-like database backed by SQLite.
// Provides access to data structures like keys, strings, and hashes.
//
// DB is safe for concurrent use by multiple goroutines as long as you use
// a single instance of DB throughout your program.
type DB struct {
	*sqlx.DB[*Tx]
	hashDB   *rhash.DB
	keyDB    *rkey.DB
	listDB   *rlist.DB
	setDB    *rset.DB
	stringDB *rstring.DB
	zsetDB   *rzset.DB
	bg       *time.Ticker
	log      *slog.Logger
}

// Open opens a new or existing database at the given path.
// Creates the database schema if necessary.
//
// The returned [DB] is safe for concurrent use by multiple goroutines
// as long as you use a single instance throughout your program.
// Typically, you only close the DB when the program exits.
//
// The opts parameter is optional. If nil, uses default options.
func Open(path string, opts *Options) (*DB, error) {
	// Apply the default options if necessary.
	opts = applyOptions(defaultOptions, opts)

	// Open the read-write database handle.
	dataSource := sqlx.DataSource(path, true, opts.Pragma)
	rw, err := sql.Open(opts.DriverName, dataSource)
	if err != nil {
		return nil, err
	}

	// Open the read-only database handle.
	dataSource = sqlx.DataSource(path, false, opts.Pragma)
	ro, err := sql.Open(opts.DriverName, dataSource)
	if err != nil {
		return nil, err
	}

	// Create the database-backed repository.
	sdb, err := sqlx.Open(rw, ro, newTx, opts.Pragma)
	if err != nil {
		return nil, err
	}

	return new(sdb, opts)
}

// OpenRead opens an existing database at the given path in read-only mode.
func OpenRead(path string, opts *Options) (*DB, error) {
	// Apply the default options if necessary.
	opts = applyOptions(defaultOptions, opts)
	opts.readonly = true

	// Open the read-only database handle.
	dataSource := sqlx.DataSource(path, false, opts.Pragma)
	db, err := sql.Open(opts.DriverName, dataSource)
	if err != nil {
		return nil, err
	}

	// Create the database-backed repository.
	sdb := sqlx.New(db, db, newTx)
	return new(sdb, opts)
}

// OpenDB connects to an existing SQL database.
// Creates the database schema if necessary.
// The opts parameter is optional. If nil, uses default options.
func OpenDB(rw *sql.DB, ro *sql.DB, opts *Options) (*DB, error) {
	opts = applyOptions(defaultOptions, opts)
	sdb, err := sqlx.Open(rw, ro, newTx, opts.Pragma)
	if err != nil {
		return nil, err
	}
	return new(sdb, opts)
}

// OpenReadDB connects to an existing SQL database in read-only mode.
func OpenReadDB(db *sql.DB, opts *Options) (*DB, error) {
	opts = applyOptions(defaultOptions, opts)
	opts.readonly = true
	sdb := sqlx.New(db, db, newTx)
	return new(sdb, opts)
}

// new creates a new database.
func new(sdb *sqlx.DB[*Tx], opts *Options) (*DB, error) {
	rdb := &DB{
		DB:       sdb,
		hashDB:   rhash.New(sdb.RW, sdb.RO),
		keyDB:    rkey.New(sdb.RW, sdb.RO),
		listDB:   rlist.New(sdb.RW, sdb.RO),
		setDB:    rset.New(sdb.RW, sdb.RO),
		stringDB: rstring.New(sdb.RW, sdb.RO),
		zsetDB:   rzset.New(sdb.RW, sdb.RO),
		log:      opts.Logger,
	}
	if !opts.readonly {
		rdb.bg = rdb.startBgManager()
	}
	return rdb, nil
}

// Hash returns the hash repository.
// A hash (hashmap) is a field-value map associated with a key.
// Use the hash repository to work with individual hashmaps
// and their fields.
func (db *DB) Hash() *rhash.DB {
	return db.hashDB
}

// Key returns the key repository.
// A key is a unique identifier for a data structure
// (string, list, hash, etc.). Use the key repository
// to manage all keys regardless of their type.
func (db *DB) Key() *rkey.DB {
	return db.keyDB
}

// List returns the list repository.
// A list is a sequence of strings ordered by insertion order.
// Use the list repository to work with lists and their elements.
func (db *DB) List() *rlist.DB {
	return db.listDB
}

// Set returns the set repository.
// A set is an unordered collection of unique strings.
// Use the set repository to work with individual sets
// and their elements, and to perform set operations.
func (db *DB) Set() *rset.DB {
	return db.setDB
}

// Str returns the string repository.
// A string is a slice of bytes associated with a key.
// Use the string repository to work with individual strings.
func (db *DB) Str() *rstring.DB {
	return db.stringDB
}

// ZSet returns the sorted set repository.
// A sorted set (zset) is a like a set, but each element has a score,
// and elements are ordered by score from low to high.
// Use the sorted set repository to work with individual sets
// and their elements, and to perform set operations.
func (db *DB) ZSet() *rzset.DB {
	return db.zsetDB
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
	if db.bg != nil {
		db.bg.Stop()
	}
	var allErr error
	if err := db.RW.Close(); err != nil {
		allErr = err
	}
	if err := db.RO.Close(); allErr == nil {
		allErr = err
	}
	return allErr
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
	hashTx *rhash.Tx
	keyTx  *rkey.Tx
	listTx *rlist.Tx
	setTx  *rset.Tx
	strTx  *rstring.Tx
	zsetTx *rzset.Tx
}

// newTx creates a new database transaction.
func newTx(tx sqlx.Tx) *Tx {
	return &Tx{tx: tx,
		hashTx: rhash.NewTx(tx),
		keyTx:  rkey.NewTx(tx),
		listTx: rlist.NewTx(tx),
		setTx:  rset.NewTx(tx),
		strTx:  rstring.NewTx(tx),
		zsetTx: rzset.NewTx(tx),
	}
}

// Hash returns the hash transaction.
func (tx *Tx) Hash() *rhash.Tx {
	return tx.hashTx
}

// Keys returns the key transaction.
func (tx *Tx) Key() *rkey.Tx {
	return tx.keyTx
}

// List returns the list transaction.
func (tx *Tx) List() *rlist.Tx {
	return tx.listTx
}

// Set returns the set transaction.
func (tx *Tx) Set() *rset.Tx {
	return tx.setTx
}

// Str returns the string transaction.
func (tx *Tx) Str() *rstring.Tx {
	return tx.strTx
}

// ZSet returns the sorted set transaction.
func (tx *Tx) ZSet() *rzset.Tx {
	return tx.zsetTx
}

// applyOptions applies custom options to the
// default options and returns the result.
func applyOptions(opts Options, custom *Options) *Options {
	if custom == nil {
		return &opts
	}
	if custom.DriverName != "" {
		opts.DriverName = custom.DriverName
	}
	if custom.Pragma != nil {
		opts.Pragma = custom.Pragma
	}
	if custom.Logger != nil {
		opts.Logger = custom.Logger
	}
	return &opts
}
