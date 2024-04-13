// Package Redka implements Redis-like database backed by SQLite.
// It provides data structures like [Keys], [Strings], and [Hashes],
// and an API to interact with them.
//
// Typically, you open a database with [Open] and use the returned
// [DB] instance methods like [DB.Key] or [DB.Str] to access the
// data structures. You should only use one instance of DB throughout
// your program and close it with [DB.Close] when the program exits.
package redka

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rhash"
	"github.com/nalgeon/redka/internal/rkey"
	"github.com/nalgeon/redka/internal/rstring"
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

// Keys provide methods to interact with keys in the database.
type Keys interface {
	// Exists reports whether the key exists.
	Exists(key string) (bool, error)

	// Count returns the number of existing keys among specified.
	Count(keys ...string) (int, error)

	// Keys returns all keys matching pattern.
	// Supports glob-style patterns like these:
	//
	//	key*  k?y  k[bce]y  k[!a-c][y-z]
	//
	// Use this method only if you are sure that the number of keys is
	// limited. Otherwise, use the [Keys.Scan] or [Keys.Scanner] methods.
	Keys(pattern string) ([]Key, error)

	// Scan iterates over keys matching pattern.
	// It returns the next pageSize keys based on the current state of the cursor.
	// Returns an empty slice when there are no more keys.
	// See [Keys.Keys] for pattern description.
	// Set pageSize = 0 for default page size.
	Scan(cursor int, pattern string, pageSize int) (rkey.ScanResult, error)

	// Scanner returns an iterator for keys matching pattern.
	// The scanner returns keys one by one, fetching keys from the
	// database in pageSize batches when necessary.
	// See [Keys.Keys] for pattern description.
	// Set pageSize = 0 for default page size.
	Scanner(pattern string, pageSize int) *rkey.Scanner

	// Random returns a random key.
	Random() (Key, error)

	// Get returns a specific key with all associated details.
	Get(key string) (Key, error)

	// Expire sets a time-to-live (ttl) for the key using a relative duration.
	// After the ttl passes, the key is expired and no longer exists.
	// Returns false is the key does not exist.
	Expire(key string, ttl time.Duration) (bool, error)

	// ExpireAt sets an expiration time for the key. After this time,
	// the key is expired and no longer exists.
	// Returns false is the key does not exist.
	ExpireAt(key string, at time.Time) (bool, error)

	// Persist removes the expiration time for the key.
	// Returns false is the key does not exist.
	Persist(key string) (bool, error)

	// Rename changes the key name.
	// If there is an existing key with the new name, it is replaced.
	Rename(key, newKey string) error

	// RenameNotExists changes the key name.
	// If there is an existing key with the new name, does nothing.
	// Returns true if the key was renamed, false otherwise.
	RenameNotExists(key, newKey string) (bool, error)

	// Delete deletes keys and their values, regardless of the type.
	// Returns the number of deleted keys. Non-existing keys are ignored.
	Delete(keys ...string) (int, error)

	// DeleteAll deletes all keys and their values, effectively resetting
	// the database. DeleteAll is not allowed inside a transaction.
	DeleteAll() error
}

// Strings provide methods to interact with strings in the database.
type Strings interface {
	// Get returns the value of the key.
	// Returns nil if the key does not exist.
	Get(key string) (Value, error)

	// GetMany returns a map of values for given keys.
	// Returns nil for keys that do not exist.
	GetMany(keys ...string) (map[string]Value, error)

	// Set sets the key value that will not expire.
	// Overwrites the value if the key already exists.
	Set(key string, value any) error

	// SetExpires sets the key value with an optional expiration time (if ttl > 0).
	// Overwrites the value and ttl if the key already exists.
	SetExpires(key string, value any, ttl time.Duration) error

	// SetNotExists sets the key value if the key does not exist.
	// Optionally sets the expiration time (if ttl > 0).
	// Returns true if the key was set, false if the key already exists.
	SetNotExists(key string, value any, ttl time.Duration) (bool, error)

	// SetExists sets the key value if the key exists.
	// Optionally sets the expiration time (if ttl > 0).
	// Returns true if the key was set, false if the key does not exist.
	SetExists(key string, value any, ttl time.Duration) (bool, error)

	// GetSet returns the previous value of a key after setting it to a new value.
	// Optionally sets the expiration time (if ttl > 0).
	// Overwrites the value and ttl if the key already exists.
	// Returns nil if the key did not exist.
	GetSet(key string, value any, ttl time.Duration) (Value, error)

	// SetMany sets the values of multiple keys.
	// Overwrites values for keys that already exist and
	// creates new keys/values for keys that do not exist.
	// Removes the TTL for existing keys.
	SetMany(kvals map[string]any) error

	// SetManyNX sets the values of multiple keys, but only if none
	// of them yet exist. Returns true if the keys were set, false if any
	// of them already exist.
	SetManyNX(kvals map[string]any) (bool, error)

	// Incr increments the key value by the specified amount.
	// If the key does not exist, sets it to 0 before the increment.
	// Returns the value after the increment.
	// Returns an error if the key value is not an integer.
	Incr(key string, delta int) (int, error)

	// IncrFloat increments the key value by the specified amount.
	// If the key does not exist, sets it to 0 before the increment.
	// Returns the value after the increment.
	// Returns an error if the key value is not a float.
	IncrFloat(key string, delta float64) (float64, error)
}

// Hashes provide methods to interact with hashmaps in the database.
type Hashes interface {
	// Get returns the value of a field in a hash.
	// Returns nil if the key or field does not exist.
	Get(key, field string) (Value, error)

	// GetMany returns a map of values for given fields.
	// Returns nil for fields that do not exist. If the key does not exist,
	// returns a map with nil values for all fields.
	GetMany(key string, fields ...string) (map[string]Value, error)

	// Exists checks if a field exists in a hash.
	// Returns false if the key does not exist.
	Exists(key, field string) (bool, error)

	// Items returns a map of all fields and values in a hash.
	// Returns an empty map if the key does not exist.
	Items(key string) (map[string]core.Value, error)

	// Fields returns all fields in a hash.
	// Returns an empty slice if the key does not exist.
	Fields(key string) ([]string, error)

	// Values returns all values in a hash.
	// Returns an empty slice if the key does not exist.
	Values(key string) ([]Value, error)

	// Len returns the number of fields in a hash.
	// Returns 0 if the key does not exist.
	Len(key string) (int, error)

	// Scan iterates over hash items with fields matching pattern.
	// It returns the next pageSize of field-value pairs (see [rhash.HashItem])
	// based on the current state of the cursor. Returns an empty slice
	// when there are no more items or if the key does not exist.
	//
	// Supports glob-style patterns like these:
	//
	//	key*  k?y  k[bce]y  k[!a-c][y-z]
	//
	// Set pageSize = 0 for default page size.
	Scan(key string, cursor int, match string, pageSize int) (rhash.ScanResult, error)

	// Scanner returns an iterator for hash items with fields matching pattern.
	// The scanner returns items one by one, fetching them from the database
	// in pageSize batches when necessary.
	// See [Hashes.Scan] for pattern description.
	// Set pageSize = 0 for default page size.
	Scanner(key string, pattern string, pageSize int) *rhash.Scanner

	// Set creates or updates the value of a field in a hash.
	// Returns true if the field was created, false if it was updated.
	// If the key does not exist, creates it.
	Set(key, field string, value any) (bool, error)

	// SetNotExists creates the value of a field in a hash if it does not exist.
	// Returns true if the field was created, false if it already exists.
	// If the key does not exist, creates it.
	SetNotExists(key, field string, value any) (bool, error)

	// SetMany creates or updates the values of multiple fields in a hash.
	// Returns the number of fields created (as opposed to updated).
	// If the key does not exist, creates it.
	SetMany(key string, items map[string]any) (int, error)

	// Incr increments the integer value of a field in a hash.
	// If the field does not exist, sets it to 0 before the increment.
	// If the key does not exist, creates it.
	// Returns the value after the increment.
	// Returns an error if the field value is not an integer.
	Incr(key, field string, delta int) (int, error)

	// IncrFloat increments the float value of a field in a hash.
	// If the field does not exist, sets it to 0 before the increment.
	// If the key does not exist, creates it.
	// Returns the value after the increment.
	// Returns an error if the field value is not a float.
	IncrFloat(key, field string, delta float64) (float64, error)

	// Delete deletes one or more items from a hash.
	// Non-existing fields are ignored.
	// If there are no fields left in the hash, deletes the key.
	// Returns the number of fields deleted.
	// Returns 0 if the key does not exist.
	Delete(key string, fields ...string) (int, error)
}

// DB is a Redis-like database backed by SQLite.
// Provides access to data structures like [Keys], [Strings], and [Hashes].
//
// DB is safe for concurrent use by multiple goroutines as long as you use
// a single instance of DB throughout your program.
type DB struct {
	*sqlx.DB[*Tx]
	keyDB    *rkey.DB
	stringDB *rstring.DB
	hashDB   *rhash.DB
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
// [simple]: https://github.com/nalgeon/redka/blob/main/example/simple/main.go
// [modernc]: https://github.com/nalgeon/redka/blob/main/example/modernc/main.go
func Open(path string) (*DB, error) {
	db, err := sql.Open(driverName, path)
	if err != nil {
		return nil, err
	}
	return OpenDB(db)
}

// OpenDB connects to the existing database.
// Creates the database schema if necessary.
func OpenDB(db *sql.DB) (*DB, error) {
	sdb, err := sqlx.Open(db, newTx)
	if err != nil {
		return nil, err
	}
	rdb := &DB{
		DB:       sdb,
		keyDB:    rkey.New(db),
		stringDB: rstring.New(db),
		hashDB:   rhash.New(db),
		log:      slog.New(new(noopHandler)),
	}
	rdb.bg = rdb.startBgManager()
	return rdb, nil
}

// Str returns the string repository.
func (db *DB) Str() Strings {
	return db.stringDB
}

// Hash returns the hash repository.
func (db *DB) Hash() Hashes {
	return db.hashDB
}

// Key returns the key repository.
func (db *DB) Key() Keys {
	return db.keyDB
}

// Close closes the database.
// It's safe for concurrent use by multiple goroutines.
func (db *DB) Close() error {
	db.bg.Stop()
	return db.SQL.Close()
}

// DeleteAll deletes all keys and values from the database.
// Not safe for concurrent use by multiple goroutines.
func (db *DB) DeleteAll() error {
	return db.DB.DeleteAll()
}

// SetLogger sets the logger for the database.
func (db *DB) SetLogger(l *slog.Logger) {
	db.log = l
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
// Same as [DB], Tx provides access to data structures like
// [Keys], [Strings], and [Hashes]. The difference is that
// you call Tx methods within a transaction managed by
// [DB.Update] or [DB.View].
type Tx struct {
	tx     sqlx.Tx
	keyTx  *rkey.Tx
	strTx  *rstring.Tx
	hashTx *rhash.Tx
}

// newTx creates a new database transaction.
func newTx(tx sqlx.Tx) *Tx {
	return &Tx{tx: tx,
		keyTx:  rkey.NewTx(tx),
		strTx:  rstring.NewTx(tx),
		hashTx: rhash.NewTx(tx),
	}
}

// Str returns the string transaction.
func (tx *Tx) Str() Strings {
	return tx.strTx
}

// Keys returns the key transaction.
func (tx *Tx) Key() Keys {
	return tx.keyTx
}

// Hash returns the hash transaction.
func (tx *Tx) Hash() Hashes {
	return tx.hashTx
}

// noopHandler is a silent log handler.
type noopHandler struct{}

func (h *noopHandler) Enabled(context.Context, slog.Level) bool {
	return false
}
func (h *noopHandler) Handle(context.Context, slog.Record) error {
	return nil
}
func (h *noopHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}
func (h *noopHandler) WithGroup(name string) slog.Handler {
	return h
}
