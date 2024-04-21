// Package rhash is a database-backed hash repository.
// It provides methods to interact with hashmaps in the database.
package rhash

import (
	"database/sql"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

// DB is a database-backed hash repository.
// A hash (hashmap) is a field-value map associated with a key.
// Use the hash repository to work with individual hashmaps
// and their fields.
type DB struct {
	*sqlx.DB[*Tx]
}

// New connects to the hash repository.
// Does not create the database schema.
func New(db *sql.DB) *DB {
	d := sqlx.New(db, NewTx)
	return &DB{d}
}

// Delete deletes one or more items from a hash.
// Non-existing fields are ignored.
// If there are no fields left in the hash, deletes the key.
// Returns the number of fields deleted.
// Returns 0 if the key does not exist.
func (d *DB) Delete(key string, fields ...string) (int, error) {
	var count int
	err := d.Update(func(tx *Tx) error {
		var err error
		count, err = tx.Delete(key, fields...)
		return err
	})
	return count, err
}

// Exists checks if a field exists in a hash.
// Returns false if the key does not exist.
func (d *DB) Exists(key, field string) (bool, error) {
	tx := NewTx(d.SQL)
	return tx.Exists(key, field)
}

// Fields returns all fields in a hash.
// Returns an empty slice if the key does not exist.
func (d *DB) Fields(key string) ([]string, error) {
	tx := NewTx(d.SQL)
	return tx.Fields(key)
}

// Get returns the value of a field in a hash.
// Returns nil if the key or field does not exist.
func (d *DB) Get(key, field string) (core.Value, error) {
	tx := NewTx(d.SQL)
	return tx.Get(key, field)
}

// GetMany returns a map of values for given fields.
// Returns nil for fields that do not exist. If the key does not exist,
// returns a map with nil values for all fields.
func (d *DB) GetMany(key string, fields ...string) (map[string]core.Value, error) {
	tx := NewTx(d.SQL)
	return tx.GetMany(key, fields...)
}

// Incr increments the integer value of a field in a hash.
// If the field does not exist, sets it to 0 before the increment.
// If the key does not exist, creates it.
// Returns the value after the increment.
// Returns an error if the field value is not an integer.
func (d *DB) Incr(key, field string, delta int) (int, error) {
	var val int
	err := d.Update(func(tx *Tx) error {
		var err error
		val, err = tx.Incr(key, field, delta)
		return err
	})
	return val, err
}

// IncrFloat increments the float value of a field in a hash.
// If the field does not exist, sets it to 0 before the increment.
// If the key does not exist, creates it.
// Returns the value after the increment.
// Returns an error if the field value is not a float.
func (d *DB) IncrFloat(key, field string, delta float64) (float64, error) {
	var val float64
	err := d.Update(func(tx *Tx) error {
		var err error
		val, err = tx.IncrFloat(key, field, delta)
		return err
	})
	return val, err
}

// Items returns a map of all fields and values in a hash.
// Returns an empty map if the key does not exist.
func (d *DB) Items(key string) (map[string]core.Value, error) {
	tx := NewTx(d.SQL)
	return tx.Items(key)
}

// Len returns the number of fields in a hash.
// Returns 0 if the key does not exist.
func (d *DB) Len(key string) (int, error) {
	tx := NewTx(d.SQL)
	return tx.Len(key)
}

// Scan iterates over hash items with fields matching pattern.
// It returns the next pageSize of field-value pairs (see [HashItem])
// based on the current state of the cursor. Returns an empty HashItem
// slice when there are no more items or if the key does not exist.
//
// Supports glob-style patterns like these:
//
//	key*  k?y  k[bce]y  k[!a-c][y-z]
//
// Set pageSize = 0 for default page size.
func (d *DB) Scan(key string, cursor int, pattern string, pageSize int) (ScanResult, error) {
	tx := NewTx(d.SQL)
	return tx.Scan(key, cursor, pattern, pageSize)
}

// Scanner returns an iterator over items with fields matching pattern.
// The scanner returns items one by one, fetching a new page
// when the current one is exhausted. Set pageSize to 0 for default value.

// Scanner returns an iterator for hash items with fields matching pattern.
// The scanner returns items one by one, fetching them from the database
// in pageSize batches when necessary.
// See [DB.Scan] for pattern description.
// Set pageSize = 0 for default page size.
func (d *DB) Scanner(key, pattern string, pageSize int) *Scanner {
	tx := NewTx(d.SQL)
	return tx.Scanner(key, pattern, pageSize)
}

// Set creates or updates the value of a field in a hash.
// Returns true if the field was created, false if it was updated.
// If the key does not exist, creates it.
func (d *DB) Set(key, field string, value any) (bool, error) {
	var created bool
	err := d.Update(func(tx *Tx) error {
		var err error
		created, err = tx.Set(key, field, value)
		return err
	})
	return created, err
}

// SetMany creates or updates the values of multiple fields in a hash.
// Returns the number of fields created (as opposed to updated).
// If the key does not exist, creates it.
func (d *DB) SetMany(key string, items map[string]any) (int, error) {
	var count int
	err := d.Update(func(tx *Tx) error {
		var err error
		count, err = tx.SetMany(key, items)
		return err
	})
	return count, err
}

// SetNotExists creates the value of a field in a hash if it does not exist.
// Returns true if the field was created, false if it already exists.
// If the key does not exist, creates it.
func (d *DB) SetNotExists(key, field string, value any) (bool, error) {
	var created bool
	err := d.Update(func(tx *Tx) error {
		var err error
		created, err = tx.SetNotExists(key, field, value)
		return err
	})
	return created, err
}

// Values returns all values in a hash.
// Returns an empty slice if the key does not exist.
func (d *DB) Values(key string) ([]core.Value, error) {
	tx := NewTx(d.SQL)
	return tx.Values(key)
}
