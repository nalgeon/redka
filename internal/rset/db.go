// Package rset is a database-backed set repository.
// It provides methods to interact with sets in the database.
package rset

import (
	"database/sql"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

// DB is a database-backed set repository.
// A set is an unordered collection of unique strings.
// Use the set repository to work with individual sets
// and their elements, and to perform set operations.
type DB struct {
	dialect sqlx.Dialect
	ro      *sql.DB
	rw      *sql.DB
	update  func(f func(tx *Tx) error) error
}

// New connects to the set repository.
// Does not create the database schema.
func New(db *sqlx.DB) *DB {
	actor := sqlx.NewTransactor(db, NewTx)
	return &DB{dialect: db.Dialect, ro: db.RO, rw: db.RW, update: actor.Update}
}

// Add adds or updates elements in a set.
// Returns the number of elements created (as opposed to updated).
// If the key does not exist, creates it.
// If the key exists but is not a set, returns ErrKeyType.
func (d *DB) Add(key string, elems ...any) (int, error) {
	var n int
	err := d.update(func(tx *Tx) error {
		var err error
		n, err = tx.Add(key, elems...)
		return err
	})
	return n, err
}

// Delete removes elements from a set.
// Returns the number of elements removed.
// Ignores the elements that do not exist.
// Does nothing if the key does not exist or is not a set.
func (d *DB) Delete(key string, elems ...any) (int, error) {
	var n int
	err := d.update(func(tx *Tx) error {
		var err error
		n, err = tx.Delete(key, elems...)
		return err
	})
	return n, err
}

// Diff returns the difference between the first set and the rest.
// The difference consists of elements that are present in the first set
// but not in any of the rest.
// If the first key does not exist or is not a set, returns an empty slice.
// If any of the remaining keys do not exist or are not sets, ignores them.
func (d *DB) Diff(keys ...string) ([]core.Value, error) {
	tx := NewTx(d.dialect, d.ro)
	return tx.Diff(keys...)
}

// DiffStore calculates the difference between the first source set
// and the rest, and stores the result in a destination set.
// Returns the number of elements in the destination set.
// If the destination key already exists, it is fully overwritten
// (all old elements are removed and the new ones are inserted).
// If the destination key already exists and is not a set, returns ErrKeyType.
// If the first source key does not exist or is not a set, does nothing,
// except deleting the destination key if it exists.
// If any of the remaining source keys do not exist or are not sets, ignores them.
func (d *DB) DiffStore(dest string, keys ...string) (int, error) {
	var n int
	err := d.update(func(tx *Tx) error {
		var err error
		n, err = tx.DiffStore(dest, keys...)
		return err
	})
	return n, err
}

// Exists reports whether the element belongs to a set.
// If the key does not exist or is not a set, returns false.
func (d *DB) Exists(key, elem any) (bool, error) {
	tx := NewTx(d.dialect, d.ro)
	return tx.Exists(key, elem)
}

// Inter returns the intersection of multiple sets.
// The intersection consists of elements that exist in all given sets.
// If any of the source keys do not exist or are not sets,
// returns an empty slice.
func (d *DB) Inter(keys ...string) ([]core.Value, error) {
	tx := NewTx(d.dialect, d.ro)
	return tx.Inter(keys...)
}

// InterStore intersects multiple sets and stores the result in a destination set.
// Returns the number of elements in the destination set.
// If the destination key already exists, it is fully overwritten
// (all old elements are removed and the new ones are inserted).
// If the destination key already exists and is not a set, returns ErrKeyType.
// If any of the source keys do not exist or are not sets, does nothing,
// except deleting the destination key if it exists.
func (d *DB) InterStore(dest string, keys ...string) (int, error) {
	var n int
	err := d.update(func(tx *Tx) error {
		var err error
		n, err = tx.InterStore(dest, keys...)
		return err
	})
	return n, err
}

// Items returns all elements in a set.
// If the key does not exist or is not a set, returns an empty slice.
func (d *DB) Items(key string) ([]core.Value, error) {
	tx := NewTx(d.dialect, d.ro)
	return tx.Items(key)
}

// Len returns the number of elements in a set.
// Returns 0 if the key does not exist or is not a set.
func (d *DB) Len(key string) (int, error) {
	tx := NewTx(d.dialect, d.ro)
	return tx.Len(key)
}

// Move moves an element from one set to another.
// If the element does not exist in the source set, returns ErrNotFound.
// If the source key does not exist or is not a set, returns ErrNotFound.
// If the destination key does not exist, creates it.
// If the destination key exists but is not a set, returns ErrKeyType.
// If the element already exists in the destination set,
// only deletes it from the source set.
func (d *DB) Move(src, dest string, elem any) error {
	err := d.update(func(tx *Tx) error {
		return tx.Move(src, dest, elem)
	})
	return err
}

// Pop removes and returns a random element from a set.
// If the key does not exist or is not a set, returns ErrNotFound.
func (d *DB) Pop(key string) (core.Value, error) {
	var v core.Value
	err := d.update(func(tx *Tx) error {
		var err error
		v, err = tx.Pop(key)
		return err
	})
	return v, err
}

// Random returns a random element from a set.
// If the key does not exist or is not a set, returns ErrNotFound.
func (d *DB) Random(key string) (core.Value, error) {
	tx := NewTx(d.dialect, d.ro)
	return tx.Random(key)
}

// Scan iterates over set elements matching pattern.
// Returns a slice of elements of size count based on the current state
// of the cursor. Returns an empty slice when there are no more items.
// If the key does not exist or is not a set, returns an empty slice.
// Supports glob-style patterns. Set count = 0 for default page size.
func (d *DB) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	tx := NewTx(d.dialect, d.ro)
	return tx.Scan(key, cursor, pattern, count)
}

// Scanner returns an iterator over set elements matching pattern.
// The scanner returns items one by one, fetching them from the database
// in pageSize batches when necessary. Stops when there are no more items
// or an error occurs. If the key does not exist or is not a set, stops immediately.
// Supports glob-style patterns. Set pageSize = 0 for default page size.
func (d *DB) Scanner(key, pattern string, pageSize int) *Scanner {
	tx := NewTx(d.dialect, d.ro)
	return tx.Scanner(key, pattern, pageSize)
}

// Union returns the union of multiple sets.
// The union consists of elements that exist in any of the given sets.
// Ignores the keys that do not exist or are not sets.
// If no keys exist, returns an empty slice.
func (d *DB) Union(keys ...string) ([]core.Value, error) {
	tx := NewTx(d.dialect, d.ro)
	return tx.Union(keys...)
}

// UnionStore unions multiple sets and stores the result in a destination set.
// Returns the number of elements in the destination set.
// If the destination key already exists, it is fully overwritten
// (all old elements are removed and the new ones are inserted).
// If the destination key already exists and is not a set, returns ErrKeyType.
// Ignores the source keys that do not exist or are not sets.
// If all of the source keys do not exist or are not sets, does nothing,
// except deleting the destination key if it exists.
func (d *DB) UnionStore(dest string, keys ...string) (int, error) {
	var n int
	err := d.update(func(tx *Tx) error {
		var err error
		n, err = tx.UnionStore(dest, keys...)
		return err
	})
	return n, err
}
