// Redis database in SQLite.
package redka

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

const driverName = "sqlite3"

const memoryURI = "file:redka?mode=memory&cache=shared"

const sqlDbFlush = `
pragma writable_schema = 1;
delete from sqlite_schema
  where name like 'rkey%' or name like 'rstring%';
pragma writable_schema = 0;
vacuum;
pragma integrity_check;`

// Redka is a Redis-like repository.
type Redka interface {
	Key() Keys
	Str() Strings
}

// DB is a Redis-like database backed by SQLite.
type DB struct {
	*sqlDB[*Tx]
	keyDB    *KeyDB
	stringDB *StringDB
	bg       *time.Ticker
	log      *slog.Logger
}

// Open opens a new or existing database at the given path.
// Creates the database schema if necessary.
func Open(path string) (*DB, error) {
	// Use in-memory database by default.
	if path == "" {
		path = memoryURI
	}
	db, err := sql.Open(driverName, path)
	if err != nil {
		return nil, err
	}
	return OpenDB(db)
}

// OpenDB connects to the database.
// Creates the database schema if necessary.
func OpenDB(db *sql.DB) (*DB, error) {
	sdb, err := openSQL(db, newTx)
	if err != nil {
		return nil, err
	}
	rdb := &DB{
		sqlDB:    sdb,
		keyDB:    newKeyDB(db),
		stringDB: newStringDB(db),
		log:      slog.New(new(noopHandler)),
	}
	rdb.bg = rdb.startBgManager()
	return rdb, nil
}

// Str returns the string repository.
func (db *DB) Str() Strings {
	return db.stringDB
}

// Key returns the key repository.
func (db *DB) Key() Keys {
	return db.keyDB
}

// Close closes the database.
func (db *DB) Close() error {
	db.bg.Stop()
	return db.db.Close()
}

// Flush deletes all keys and values from the database.
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, err := db.db.Exec(sqlDbFlush)
	if err != nil {
		return err
	}

	err = db.init()
	if err != nil {
		return err
	}

	return nil
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
			count, err := db.keyDB.deleteExpired(nKeys)
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
	tx    sqlTx
	keyTx *KeyTx
	strTx *StringTx
}

func (tx *Tx) Str() Strings {
	return tx.strTx
}

func (tx *Tx) Key() Keys {
	return tx.keyTx
}

// newTx creates a new database transaction.
func newTx(tx sqlTx) *Tx {
	return &Tx{tx: tx, keyTx: newKeyTx(tx), strTx: newStringTx(tx)}
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
