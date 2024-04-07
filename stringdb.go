// Redis-like string repository in SQLite.
package redka

import (
	"database/sql"
	"time"
)

// Strings is a string repository.
type Strings interface {
	Get(key string) (Value, error)
	GetMany(keys ...string) ([]Value, error)
	Set(key string, value any) error
	SetExpires(key string, value any, ttl time.Duration) error
	SetNotExists(key string, value any, ttl time.Duration) (bool, error)
	SetExists(key string, value any, ttl time.Duration) (bool, error)
	GetSet(key string, value any, ttl time.Duration) (Value, error)
	SetMany(kvals ...KeyValue) error
	SetManyNX(kvals ...KeyValue) (bool, error)
	Length(key string) (int, error)
	GetRange(key string, start, end int) (Value, error)
	SetRange(key string, offset int, value string) (int, error)
	Append(key, value string) (int, error)
	Incr(key string, delta int) (int, error)
	IncrFloat(key string, delta float64) (float64, error)
	Delete(keys ...string) (int, error)
}

// StringDB is a database-backed string repository.
type StringDB struct {
	*sqlDB[*StringTx]
}

// newStringDB connects to the string repository.
// Does not create the database schema.
func newStringDB(db *sql.DB) *StringDB {
	d := newSqlDB(db, newStringTx)
	return &StringDB{d}
}

// Get returns the value of the key.
func (d *StringDB) Get(key string) (Value, error) {
	tx := newStringTx(d.db)
	return tx.Get(key)
}

// GetMany returns the values of multiple keys.
func (d *StringDB) GetMany(keys ...string) ([]Value, error) {
	tx := newStringTx(d.db)
	return tx.GetMany(keys...)
}

// Set sets the key value. The key does not expire.
func (d *StringDB) Set(key string, value any) error {
	err := d.Update(func(tx *StringTx) error {
		return tx.Set(key, value)
	})
	return err
}

// SetExpires sets the key value with an optional expiration time (if ttl > 0).
func (d *StringDB) SetExpires(key string, value any, ttl time.Duration) error {
	err := d.Update(func(tx *StringTx) error {
		return tx.SetExpires(key, value, ttl)
	})
	return err
}

// SetNotExists sets the key value if the key does not exist.
// Optionally sets the expiration time (if ttl > 0).
func (d *StringDB) SetNotExists(key string, value any, ttl time.Duration) (bool, error) {
	var ok bool
	err := d.Update(func(tx *StringTx) error {
		var err error
		ok, err = tx.SetNotExists(key, value, ttl)
		return err
	})
	return ok, err
}

// SetExists sets the key value if the key exists.
func (d *StringDB) SetExists(key string, value any, ttl time.Duration) (bool, error) {
	var ok bool
	err := d.Update(func(tx *StringTx) error {
		var err error
		ok, err = tx.SetExists(key, value, ttl)
		return err
	})
	return ok, err
}

// GetSet returns the previous value of a key after setting it to a new value.
// Optionally sets the expiration time (if ttl > 0).
func (d *StringDB) GetSet(key string, value any, ttl time.Duration) (Value, error) {
	var val Value
	err := d.Update(func(tx *StringTx) error {
		var err error
		val, err = tx.GetSet(key, value, ttl)
		return err
	})
	return val, err
}

// SetMany sets the values of multiple keys.
func (d *StringDB) SetMany(kvals ...KeyValue) error {
	err := d.Update(func(tx *StringTx) error {
		return tx.SetMany(kvals...)
	})
	return err
}

// SetManyNX sets the values of multiple keys,
// but only if none of them yet exist.
func (d *StringDB) SetManyNX(kvals ...KeyValue) (bool, error) {
	var ok bool
	err := d.Update(func(tx *StringTx) error {
		var err error
		ok, err = tx.SetManyNX(kvals...)
		return err
	})
	return ok, err
}

// Length returns the length of the key value.
func (d *StringDB) Length(key string) (int, error) {
	tx := newStringTx(d.db)
	return tx.Length(key)
}

// GetRange returns the substring of the key value.
func (d *StringDB) GetRange(key string, start, end int) (Value, error) {
	tx := newStringTx(d.db)
	return tx.GetRange(key, start, end)
}

// SetRange overwrites part of the key value.
func (d *StringDB) SetRange(key string, offset int, value string) (int, error) {
	var n int
	err := d.Update(func(tx *StringTx) error {
		var err error
		n, err = tx.SetRange(key, offset, value)
		return err
	})
	return n, err
}

// Append appends the value to the key.
func (d *StringDB) Append(key, value string) (int, error) {
	var n int
	err := d.Update(func(tx *StringTx) error {
		var err error
		n, err = tx.Append(key, value)
		return err
	})
	return n, err
}

// Incr increments the key value by the specified amount.
func (d *StringDB) Incr(key string, delta int) (int, error) {
	var val int
	err := d.Update(func(tx *StringTx) error {
		var err error
		val, err = tx.Incr(key, delta)
		return err
	})
	return val, err
}

// IncrFloat increments the key value by the specified amount.
func (d *StringDB) IncrFloat(key string, delta float64) (float64, error) {
	var val float64
	err := d.Update(func(tx *StringTx) error {
		var err error
		val, err = tx.IncrFloat(key, delta)
		return err
	})
	return val, err
}

// Delete deletes keys and their values.
// Returns the number of deleted keys. Non-existing keys are ignored.
func (d *StringDB) Delete(keys ...string) (int, error) {
	var count int
	err := d.Update(func(tx *StringTx) error {
		var err error
		count, err = tx.Delete(keys...)
		return err
	})
	return count, err
}
