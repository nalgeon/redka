// Package rkey is a database-backed key repository.
// It provides methods to interact with keys in the database.
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

// Exists reports whether the key exists.
func (db *DB) Exists(key string) (bool, error) {
	tx := NewTx(db.SQL)
	return tx.Exists(key)
}

// Count returns the number of existing keys among specified.
func (db *DB) Count(keys ...string) (int, error) {
	tx := NewTx(db.SQL)
	return tx.Count(keys...)
}

// Keys returns all keys matching pattern.
// Supports glob-style patterns like these:
//
//	key*  k?y  k[bce]y  k[!a-c][y-z]
//
// Use this method only if you are sure that the number of keys is
// limited. Otherwise, use the [DB.Scan] or [DB.Scanner] methods.
func (db *DB) Keys(pattern string) ([]core.Key, error) {
	tx := NewTx(db.SQL)
	return tx.Keys(pattern)
}

// Scan iterates over keys matching pattern.
// It returns the next pageSize keys based on the current state of the cursor.
// Returns an empty slice when there are no more keys.
// See [DB.Keys] for pattern description.
// Set pageSize = 0 for default page size.
func (db *DB) Scan(cursor int, pattern string, pageSize int) (ScanResult, error) {
	tx := NewTx(db.SQL)
	return tx.Scan(cursor, pattern, pageSize)
}

// Scanner returns an iterator for keys matching pattern.
// The scanner returns keys one by one, fetching keys from the
// database in pageSize batches when necessary.
// See [DB.Keys] for pattern description.
// Set pageSize = 0 for default page size.
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

// Expire sets a time-to-live (ttl) for the key using a relative duration.
// After the ttl passes, the key is expired and no longer exists.
// Returns false is the key does not exist.
func (db *DB) Expire(key string, ttl time.Duration) (bool, error) {
	var ok bool
	err := db.Update(func(tx *Tx) error {
		var err error
		ok, err = tx.Expire(key, ttl)
		return err
	})
	return ok, err
}

// ExpireAt sets an expiration time for the key. After this time,
// the key is expired and no longer exists.
// Returns false is the key does not exist.
func (db *DB) ExpireAt(key string, at time.Time) (bool, error) {
	var ok bool
	err := db.Update(func(tx *Tx) error {
		var err error
		ok, err = tx.ExpireAt(key, at)
		return err
	})
	return ok, err
}

// Persist removes the expiration time for the key.
// Returns false is the key does not exist.
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
func (db *DB) Rename(key, newKey string) error {
	err := db.Update(func(tx *Tx) error {
		err := tx.Rename(key, newKey)
		return err
	})
	return err
}

// RenameNotExists changes the key name.
// If there is an existing key with the new name, does nothing.
// Returns true if the key was renamed, false otherwise.
func (db *DB) RenameNotExists(key, newKey string) (bool, error) {
	var ok bool
	err := db.Update(func(tx *Tx) error {
		var err error
		ok, err = tx.RenameNotExists(key, newKey)
		return err
	})
	return ok, err
}

// Delete deletes keys and their values, regardless of the type.
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

// DeleteAll deletes all keys and their values, effectively resetting
// the database. DeleteAll is not allowed inside a transaction.
func (db *DB) DeleteAll() error {
	return db.DB.DeleteAll()
}
