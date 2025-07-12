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
// A key is a unique identifier for a data structure
// (string, list, hash, etc.). Use the key repository
// to manage all keys regardless of their type.
type DB struct {
	dialect sqlx.Dialect
	ro      *sql.DB
	rw      *sql.DB
	update  func(f func(tx *Tx) error) error
}

// New creates a new database-backed key repository.
// Does not create the database schema.
func New(db *sqlx.DB) *DB {
	actor := sqlx.NewTransactor(db, NewTx)
	return &DB{dialect: db.Dialect, ro: db.RO, rw: db.RW, update: actor.Update}
}

// Count returns the number of existing keys among specified.
func (d *DB) Count(keys ...string) (int, error) {
	tx := NewTx(d.dialect, d.ro)
	return tx.Count(keys...)
}

// Delete deletes keys and their values, regardless of the type.
// Returns the number of deleted keys. Non-existing keys are ignored.
func (d *DB) Delete(keys ...string) (int, error) {
	tx := NewTx(d.dialect, d.rw)
	return tx.Delete(keys...)
}

// DeleteAll deletes all keys and their values, effectively resetting
// the database. Should not be run inside a database transaction.
func (d *DB) DeleteAll() error {
	tx := NewTx(d.dialect, d.rw)
	return tx.DeleteAll()
}

// DeleteExpired deletes keys with expired TTL, but no more than n keys.
// If n = 0, deletes all expired keys.
func (d *DB) DeleteExpired(n int) (count int, err error) {
	tx := NewTx(d.dialect, d.rw)
	return tx.deleteExpired(n)
}

// Exists reports whether the key exists.
func (d *DB) Exists(key string) (bool, error) {
	tx := NewTx(d.dialect, d.ro)
	return tx.Exists(key)
}

// Expire sets a time-to-live (ttl) for the key using a relative duration.
// After the ttl passes, the key is expired and no longer exists.
// If the key does not exist, returns ErrNotFound.
func (d *DB) Expire(key string, ttl time.Duration) error {
	tx := NewTx(d.dialect, d.rw)
	return tx.Expire(key, ttl)
}

// ExpireAt sets an expiration time for the key. After this time,
// the key is expired and no longer exists.
// If the key does not exist, returns ErrNotFound.
func (d *DB) ExpireAt(key string, at time.Time) error {
	tx := NewTx(d.dialect, d.rw)
	return tx.ExpireAt(key, at)
}

// Get returns a specific key with all associated details.
// If the key does not exist, returns ErrNotFound.
func (d *DB) Get(key string) (core.Key, error) {
	tx := NewTx(d.dialect, d.ro)
	return tx.Get(key)
}

// Keys returns all keys matching pattern.
// Supports glob-style patterns like these:
//
//	key*  k?y  k[bce]y  k[!a-c][y-z]
//
// Use this method only if you are sure that the number of keys is
// limited. Otherwise, use the [DB.Scan] or [DB.Scanner] methods.
func (d *DB) Keys(pattern string) ([]core.Key, error) {
	tx := NewTx(d.dialect, d.ro)
	return tx.Keys(pattern)
}

// Len returns the total number of keys, including expired ones.
func (d *DB) Len() (int, error) {
	tx := NewTx(d.dialect, d.ro)
	return tx.Len()
}

// Persist removes the expiration time for the key.
// If the key does not exist, returns ErrNotFound.
func (d *DB) Persist(key string) error {
	tx := NewTx(d.dialect, d.rw)
	return tx.Persist(key)
}

// Random returns a random key.
// If there are no keys, returns ErrNotFound.
func (d *DB) Random() (core.Key, error) {
	tx := NewTx(d.dialect, d.ro)
	return tx.Random()
}

// Rename changes the key name.
// If there is an existing key with the new name, it is replaced.
// If the old key does not exist, returns ErrNotFound.
func (d *DB) Rename(key, newKey string) error {
	err := d.update(func(tx *Tx) error {
		err := tx.Rename(key, newKey)
		return err
	})
	return err
}

// RenameNotExists changes the key name.
// If there is an existing key with the new name, does nothing.
// Returns true if the key was renamed, false otherwise.
func (d *DB) RenameNotExists(key, newKey string) (bool, error) {
	var ok bool
	err := d.update(func(tx *Tx) error {
		var err error
		ok, err = tx.RenameNotExists(key, newKey)
		return err
	})
	return ok, err
}

// Scan iterates over keys matching pattern.
// Returns a slice of keys (see [core.Key]) of size count
// based on the current state of the cursor.
// Returns an empty slice when there are no more keys.
//
// Filtering and limiting options:
//   - pattern (glob-style) to filter keys by name (* = any name).
//   - ktype to filter keys by type (TypeAny = any type).
//   - count to limit the number of keys returned (0 = default).
func (d *DB) Scan(cursor int, pattern string, ktype core.TypeID, count int) (ScanResult, error) {
	tx := NewTx(d.dialect, d.ro)
	return tx.Scan(cursor, pattern, ktype, count)
}

// Scanner returns an iterator for keys matching pattern.
// The scanner returns keys one by one, fetching them
// from the database in pageSize batches when necessary.
// Stops when there are no more items or an error occurs.
//
// Filtering and pagination options:
//   - pattern (glob-style) to filter keys by name (* = any name).
//   - ktype to filter keys by type (TypeAny = any type).
//   - pageSize to limit the number of keys fetched at once (0 = default).
func (d *DB) Scanner(pattern string, ktype core.TypeID, pageSize int) *Scanner {
	tx := NewTx(d.dialect, d.ro)
	return newScanner(tx, pattern, ktype, pageSize)
}
