// Package rstring is a database-backed string repository.
// It provides methods to interact with strings in the database.
package rstring

import (
	"database/sql"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

// DB is a database-backed string repository.
// A string is a slice of bytes associated with a key.
// Use the string repository to work with individual strings.
type DB struct {
	ro     *sql.DB
	rw     *sql.DB
	update func(f func(tx *Tx) error) error
}

// New connects to the string repository.
// Does not create the database schema.
func New(db *sqlx.DB) *DB {
	actor := sqlx.NewTransactor(db, NewTx)
	return &DB{ro: db.RO, rw: db.RW, update: actor.Update}
}

// Get returns the value of the key.
// If the key does not exist or is not a string, returns ErrNotFound.
func (d *DB) Get(key string) (core.Value, error) {
	tx := NewTx(d.ro)
	return tx.Get(key)
}

// GetMany returns a map of values for given keys.
// Ignores keys that do not exist or not strings,
// and does not return them in the map.
func (d *DB) GetMany(keys ...string) (map[string]core.Value, error) {
	tx := NewTx(d.ro)
	return tx.GetMany(keys...)
}

// Incr increments the integer key value by the specified amount.
// Returns the value after the increment.
// If the key does not exist, sets it to 0 before the increment.
// If the key value is not an integer, returns ErrValueType.
// If the key exists but is not a string, returns ErrKeyType.
func (d *DB) Incr(key string, delta int) (int, error) {
	var val int
	err := d.update(func(tx *Tx) error {
		var err error
		val, err = tx.Incr(key, delta)
		return err
	})
	return val, err
}

// IncrFloat increments the float key value by the specified amount.
// Returns the value after the increment.
// If the key does not exist, sets it to 0 before the increment.
// If the key value is not an float, returns ErrValueType.
// If the key exists but is not a string, returns ErrKeyType.
func (d *DB) IncrFloat(key string, delta float64) (float64, error) {
	var val float64
	err := d.update(func(tx *Tx) error {
		var err error
		val, err = tx.IncrFloat(key, delta)
		return err
	})
	return val, err
}

// Set sets the key value that will not expire.
// Overwrites the value if the key already exists.
// If the key exists but is not a string, returns ErrKeyType.
func (d *DB) Set(key string, value any) error {
	err := d.update(func(tx *Tx) error {
		return tx.Set(key, value)
	})
	return err
}

// SetExpires sets the key value with an optional expiration time (if ttl > 0).
// Overwrites the value and ttl if the key already exists.
// If the key exists but is not a string, returns ErrKeyType.
func (d *DB) SetExpires(key string, value any, ttl time.Duration) error {
	err := d.update(func(tx *Tx) error {
		return tx.SetExpires(key, value, ttl)
	})
	return err
}

// SetMany sets the values of multiple keys.
// Overwrites values for keys that already exist and
// creates new keys/values for keys that do not exist.
// Removes the TTL for existing keys.
// If any of the keys exists but is not a string, returns ErrKeyType.
func (d *DB) SetMany(items map[string]any) error {
	err := d.update(func(tx *Tx) error {
		return tx.SetMany(items)
	})
	return err
}

// SetWith sets the key value with additional options.
func (d *DB) SetWith(key string, value any) SetCmd {
	return SetCmd{db: d, key: key, val: value}
}
