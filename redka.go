// Redis database in SQLite.
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

// Errors that can be returned by the commands.
var ErrKeyNotFound = core.ErrKeyNotFound
var ErrInvalidType = core.ErrInvalidType

// Key represents a key data structure.
type Key = core.Key

// Value represents a key value (a byte slice).
// It can be converted to other scalar types.
type Value = core.Value

// Keys is a key repository.
type Keys interface {
	Exists(key string) (bool, error)
	Count(keys ...string) (int, error)
	Search(pattern string) ([]Key, error)
	Scan(cursor int, pattern string, count int) (rkey.ScanResult, error)
	Scanner(pattern string, pageSize int) *rkey.Scanner
	Random() (Key, error)
	Get(key string) (Key, error)
	Expire(key string, ttl time.Duration) (bool, error)
	ExpireAt(key string, at time.Time) (bool, error)
	Persist(key string) (bool, error)
	Rename(key, newKey string) (bool, error)
	RenameNX(key, newKey string) (bool, error)
	Delete(keys ...string) (int, error)
	DeleteAll() error
}

// Strings is a string repository.
type Strings interface {
	Get(key string) (Value, error)
	GetMany(keys ...string) ([]Value, error)
	Set(key string, value any) error
	SetExpires(key string, value any, ttl time.Duration) error
	SetNotExists(key string, value any, ttl time.Duration) (bool, error)
	SetExists(key string, value any, ttl time.Duration) (bool, error)
	GetSet(key string, value any, ttl time.Duration) (Value, error)
	SetMany(kvals map[string]any) error
	SetManyNX(kvals map[string]any) (bool, error)
	Incr(key string, delta int) (int, error)
	IncrFloat(key string, delta float64) (float64, error)
	Delete(keys ...string) (int, error)
}

// Hashes is a hash repository.
type Hashes interface {
	Get(key, field string) (Value, error)
	GetMany(key string, fields ...string) (map[string]Value, error)
	Exists(key, field string) (bool, error)
	Items(key string) (map[string]core.Value, error)
	Fields(key string) ([]string, error)
	Values(key string) ([]Value, error)
	Len(key string) (int, error)
	Scan(key string, cursor int, match string, count int) (rhash.ScanResult, error)
	Scanner(key string, pattern string, pageSize int) *rhash.Scanner
	Set(key, field string, value any) (bool, error)
	SetNotExists(key, field string, value any) (bool, error)
	SetMany(key string, items map[string]any) (int, error)
	Incr(key, field string, delta int) (int, error)
	IncrFloat(key, field string, delta float64) (float64, error)
	Delete(key string, fields ...string) (int, error)
}

// DB is a Redis-like database backed by SQLite.
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
func Open(path string) (*DB, error) {
	db, err := sql.Open(driverName, path)
	if err != nil {
		return nil, err
	}
	return OpenDB(db)
}

// OpenDB connects to the database.
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
func (db *DB) Close() error {
	db.bg.Stop()
	return db.SQL.Close()
}

// DeleteAll deletes all keys and values from the database.
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
func (tx *Tx) Str() Strings {
	return tx.strTx
}
func (tx *Tx) Key() Keys {
	return tx.keyTx
}
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
