package rhash

import (
	"database/sql"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

// DB is a database-backed hash repository.
type DB struct {
	*sqlx.DB[*Tx]
}

// New connects to the hash repository.
// Does not create the database schema.
func New(db *sql.DB) *DB {
	d := sqlx.New(db, NewTx)
	return &DB{d}
}

// Get returns the value of a field in a hash.
func (d *DB) Get(key, field string) (core.Value, error) {
	tx := NewTx(d.SQL)
	return tx.Get(key, field)
}

// GetMany returns the values of multiple fields in a hash.
func (d *DB) GetMany(key string, fields ...string) (map[string]core.Value, error) {
	tx := NewTx(d.SQL)
	return tx.GetMany(key, fields...)
}

// Exists checks if a field exists in a hash.
func (d *DB) Exists(key, field string) (bool, error) {
	tx := NewTx(d.SQL)
	return tx.Exists(key, field)
}

// Items returns all fields and values in a hash.
func (d *DB) Items(key string) (map[string]core.Value, error) {
	tx := NewTx(d.SQL)
	return tx.Items(key)
}

// Fields returns all fields in a hash.
func (d *DB) Fields(key string) ([]string, error) {
	tx := NewTx(d.SQL)
	return tx.Fields(key)
}

// Values returns all values in a hash.
func (d *DB) Values(key string) ([]core.Value, error) {
	tx := NewTx(d.SQL)
	return tx.Values(key)
}

// Len returns the number of fields in a hash.
func (d *DB) Len(key string) (int, error) {
	tx := NewTx(d.SQL)
	return tx.Len(key)
}

// Scan iterates over items with fields matching pattern
// by returning the next page based on the current state of the cursor.
// Count regulates the page size (count = 0 for default value).
func (d *DB) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	tx := NewTx(d.SQL)
	return tx.Scan(key, cursor, pattern, count)
}

// Scanner returns an iterator over items with fields matching pattern.
// The scanner returns items one by one, fetching a new page
// when the current one is exhausted. Set pageSize to 0 for default value.
func (d *DB) Scanner(key, pattern string, pageSize int) *Scanner {
	tx := NewTx(d.SQL)
	return tx.Scanner(key, pattern, pageSize)
}

// Set creates or updates the value of a field in a hash.
// Returns true if the field was created, false if it was updated.
func (d *DB) Set(key, field string, value any) (bool, error) {
	var created bool
	err := d.Update(func(tx *Tx) error {
		var err error
		created, err = tx.Set(key, field, value)
		return err
	})
	return created, err
}

// SetNotExists creates the value of a field in a hash if it does not exist.
func (d *DB) SetNotExists(key, field string, value any) (bool, error) {
	var created bool
	err := d.Update(func(tx *Tx) error {
		var err error
		created, err = tx.SetNotExists(key, field, value)
		return err
	})
	return created, err
}

// SetMany creates or updates the values of multiple fields in a hash.
// Returns the number of fields created (as opposed to updated).
func (d *DB) SetMany(key string, items map[string]any) (int, error) {
	var count int
	err := d.Update(func(tx *Tx) error {
		var err error
		count, err = tx.SetMany(key, items)
		return err
	})
	return count, err
}

// Incr increments the integer value of a field in a hash.
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
func (d *DB) IncrFloat(key, field string, delta float64) (float64, error) {
	var val float64
	err := d.Update(func(tx *Tx) error {
		var err error
		val, err = tx.IncrFloat(key, field, delta)
		return err
	})
	return val, err
}

// Delete deletes one or more items from a hash.
func (d *DB) Delete(key string, fields ...string) (int, error) {
	var count int
	err := d.Update(func(tx *Tx) error {
		var err error
		count, err = tx.Delete(key, fields...)
		return err
	})
	return count, err
}
