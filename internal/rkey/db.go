// Redis-like key repository in SQLite.
package rkey

import (
	"database/sql"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

// DB is a database-backed key repository.
type DB struct {
	*sqlx.DB[*Tx]
}

// New creates a new database-backed key repository.
// Does not create the database schema.
func New(db *sql.DB) *DB {
	d := sqlx.New(db, NewTx)
	return &DB{d}
}

// Exists returns the number of existing keys among specified.
func (db *DB) Exists(keys ...string) (int, error) {
	tx := NewTx(db.SQL)
	return tx.Exists(keys...)
}

// Search returns all keys matching pattern.
func (db *DB) Search(pattern string) ([]core.Key, error) {
	tx := NewTx(db.SQL)
	return tx.Search(pattern)
}

// Scan iterates over keys matching pattern by returning
// the next page based on the current state of the cursor.
// Count regulates the number of keys returned (count = 0 for default).
func (db *DB) Scan(cursor int, pattern string, count int) (ScanResult, error) {
	tx := NewTx(db.SQL)
	return tx.Scan(cursor, pattern, count)
}

// Scanner returns an iterator for keys matching pattern.
// The scanner returns keys one by one, fetching a new page
// when the current one is exhausted. Set pageSize to 0 for default.
func (db *DB) Scanner(pattern string, pageSize int) *Scanner {
	return newScanner(NewTx(db.SQL), pattern, pageSize)
}

// Random returns a random key.
func (db *DB) Random() (core.Key, error) {
	tx := NewTx(db.SQL)
	return tx.Random()
}

// Get returns a specific key with all associated details.
func (db *DB) Get(key string) (core.Key, error) {
	tx := NewTx(db.SQL)
	return tx.Get(key)
}

// Expire sets a timeout on the key using a relative duration.
func (db *DB) Expire(key string, ttl time.Duration) (bool, error) {
	var ok bool
	err := db.Update(func(tx *Tx) error {
		var err error
		ok, err = tx.Expire(key, ttl)
		return err
	})
	return ok, err
}

// ExpireAt sets a timeout on the key using an absolute time.
func (db *DB) ExpireAt(key string, at time.Time) (bool, error) {
	var ok bool
	err := db.Update(func(tx *Tx) error {
		var err error
		ok, err = tx.ExpireAt(key, at)
		return err
	})
	return ok, err
}

// Persist removes a timeout on the key.
func (db *DB) Persist(key string) (bool, error) {
	var ok bool
	err := db.Update(func(tx *Tx) error {
		var err error
		ok, err = tx.Persist(key)
		return err
	})
	return ok, err
}

// Rename changes the key name.
// If there is an existing key with the new name, it is replaced.
func (db *DB) Rename(key, newKey string) (bool, error) {
	var ok bool
	err := db.Update(func(tx *Tx) error {
		var err error
		ok, err = tx.Rename(key, newKey)
		return err
	})
	return ok, err
}

// RenameIfNotExists changes the key name.
// If there is an existing key with the new name, does nothing.
func (db *DB) RenameNX(key, newKey string) (bool, error) {
	var ok bool
	err := db.Update(func(tx *Tx) error {
		var err error
		ok, err = tx.RenameNX(key, newKey)
		return err
	})
	return ok, err
}

// Delete deletes keys and their values.
// Returns the number of deleted keys. Non-existing keys are ignored.
func (db *DB) Delete(keys ...string) (int, error) {
	var count int
	err := db.Update(func(tx *Tx) error {
		var err error
		count, err = tx.Delete(keys...)
		return err
	})
	return count, err
}

// DeleteExpired deletes keys with expired TTL, but no more than n keys.
// If n = 0, deletes all expired keys.
func (db *DB) DeleteExpired(n int) (count int, err error) {
	err = db.Update(func(tx *Tx) error {
		count, err = tx.deleteExpired(n)
		return err
	})
	return count, err
}
