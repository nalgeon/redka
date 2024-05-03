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
func New(rw *sql.DB, ro *sql.DB) *DB {
	d := sqlx.New(rw, ro, NewTx)
	return &DB{d}
}

// Delete deletes one or more items from a hash.
// Returns the number of fields deleted.
// Ignores non-existing fields.
// Does nothing if the key does not exist or is not a hash.
func (d *DB) Delete(key string, fields ...string) (int, error) {
	var n int
	err := d.Update(func(tx *Tx) error {
		var err error
		n, err = tx.Delete(key, fields...)
		return err
	})
	return n, err
}

// Exists checks if a field exists in a hash.
// If the key does not exist or is not a hash, returns false.
func (d *DB) Exists(key, field string) (bool, error) {
	tx := NewTx(d.RO)
	return tx.Exists(key, field)
}

// Fields returns all fields in a hash.
// If the key does not exist or is not a hash, returns an empty slice.
func (d *DB) Fields(key string) ([]string, error) {
	tx := NewTx(d.RO)
	return tx.Fields(key)
}

// Get returns the value of a field in a hash.
// If the element does not exist, returns ErrNotFound.
// If the key does not exist or is not a hash, returns ErrNotFound.
func (d *DB) Get(key, field string) (core.Value, error) {
	tx := NewTx(d.RO)
	return tx.Get(key, field)
}

// GetMany returns a map of values for given fields.
// Ignores fields that do not exist and do not return them in the map.
// If the key does not exist or is not a hash, returns an empty map.
func (d *DB) GetMany(key string, fields ...string) (map[string]core.Value, error) {
	tx := NewTx(d.RO)
	return tx.GetMany(key, fields...)
}

// Incr increments the integer value of a field in a hash.
// Returns the value after the increment.
// If the field does not exist, sets it to 0 before the increment.
// If the field value is not an integer, returns ErrValueType.
// If the key does not exist, creates it.
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
// Returns the value after the increment.
// If the field does not exist, sets it to 0 before the increment.
// If the field value is not a float, returns ErrValueType.
// If the key does not exist, creates it.
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
// If the key does not exist or is not a hash, returns an empty map.
func (d *DB) Items(key string) (map[string]core.Value, error) {
	tx := NewTx(d.RO)
	return tx.Items(key)
}

// Len returns the number of fields in a hash.
// If the key does not exist or is not a hash, returns 0.
func (d *DB) Len(key string) (int, error) {
	tx := NewTx(d.RO)
	return tx.Len(key)
}

// Scan iterates over hash items with fields matching pattern.
// Returns a slice of field-value pairs (see [HashItem]) of size count
// based on the current state of the cursor. Returns an empty HashItem
// slice when there are no more items.
// If the key does not exist or is not a hash, returns a nil slice.
// Supports glob-style patterns. Set count = 0 for default page size.
func (d *DB) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	tx := NewTx(d.RO)
	return tx.Scan(key, cursor, pattern, count)
}

// Scanner returns an iterator for hash items with fields matching pattern.
// The scanner returns items one by one, fetching them from the database
// in pageSize batches when necessary. Stops when there are no more items
// or an error occurs. If the key does not exist or is not a hash, stops immediately.
// Supports glob-style patterns. Set pageSize = 0 for default page size.
func (d *DB) Scanner(key, pattern string, pageSize int) *Scanner {
	tx := NewTx(d.RO)
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
	var n int
	err := d.Update(func(tx *Tx) error {
		var err error
		n, err = tx.SetMany(key, items)
		return err
	})
	return n, err
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
// If the key does not exist or is not a hash, returns an empty slice.
func (d *DB) Values(key string) ([]core.Value, error) {
	tx := NewTx(d.RO)
	return tx.Values(key)
}
