package rstring

import (
	"database/sql"
	"slices"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rkey"
	"github.com/nalgeon/redka/internal/sqlx"
)

const (
	sqlGet = `
	select key, value
	from rstring
	  join rkey on key_id = rkey.id and (etime is null or etime > :now)
	where key = :key`

	sqlGetMany = `
	select key, value
	from rstring
	  join rkey on key_id = rkey.id and (etime is null or etime > :now)
	where key in (:keys)`

	sqlSet1 = `
	insert into rkey (key, type, version, etime, mtime)
	values (:key, :type, :version, :etime, :mtime)
	on conflict (key) do update set
	  version = version+1,
	  type = excluded.type,
	  etime = excluded.etime,
	  mtime = excluded.mtime`

	sqlSet2 = `
	insert into rstring (key_id, value)
	values ((select id from rkey where key = :key), :value)
	on conflict (key_id) do update
	set value = excluded.value`

	sqlUpdate1 = `
	insert into rkey (key, type, version, etime, mtime)
	values (:key, :type, :version, null, :mtime)
	on conflict (key) do update set
	  version = version+1,
	  type = excluded.type,
	  -- not changing etime
	  mtime = excluded.mtime`

	sqlUpdate2 = `
	insert into rstring (key_id, value)
	values ((select id from rkey where key = :key), :value)
	on conflict (key_id) do update
	set value = excluded.value`
)

// Tx is a string repository transaction.
type Tx struct {
	tx sqlx.Tx
}

// NewTx creates a string repository transaction
// from a generic database transaction.
func NewTx(tx sqlx.Tx) *Tx {
	return &Tx{tx}
}

// Get returns the value of the key.
// Returns nil if the key does not exist.
func (tx *Tx) Get(key string) (core.Value, error) {
	args := []any{
		sql.Named("key", key),
		sql.Named("now", time.Now().UnixMilli()),
	}
	row := tx.tx.QueryRow(sqlGet, args...)
	_, val, err := scanValue(row)
	return val, err
}

// GetMany returns a map of values for given keys.
// Returns nil for keys that do not exist.
func (tx *Tx) GetMany(keys ...string) (map[string]core.Value, error) {
	// Build a map of requested keys.
	items := make(map[string]core.Value, len(keys))
	for _, key := range keys {
		items[key] = nil
	}

	// Get the values of the requested keys.
	now := time.Now().UnixMilli()
	query, keyArgs := sqlx.ExpandIn(sqlGetMany, ":keys", keys)
	args := slices.Concat([]any{sql.Named("now", now)}, keyArgs)

	var rows *sql.Rows
	rows, err := tx.tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Fill the map with the values for existing keys
	// (the rest of the keys will remain nil).
	for rows.Next() {
		key, val, err := scanValue(rows)
		if err != nil {
			return nil, err
		}
		items[key] = val
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return items, nil
}

// GetSet returns the previous value of a key after setting it to a new value.
// Optionally sets the expiration time (if ttl > 0).
// Overwrites the value and ttl if the key already exists.
// Returns nil if the key did not exist.
func (tx *Tx) GetSet(key string, value any, ttl time.Duration) (core.Value, error) {
	if !core.IsValueType(value) {
		return nil, core.ErrValueType
	}

	prev, err := tx.Get(key)
	if err != nil {
		return nil, err
	}

	err = tx.set(key, value, ttl)
	return prev, err
}

// Incr increments the key value by the specified amount.
// If the key does not exist, sets it to 0 before the increment.
// Returns the value after the increment.
// Returns an error if the key value is not an integer.
func (tx *Tx) Incr(key string, delta int) (int, error) {
	// get the current value
	val, err := tx.Get(key)
	if err != nil {
		return 0, err
	}

	// check if the value is a valid integer
	valInt, err := val.Int()
	if err != nil {
		return 0, core.ErrValueType
	}

	// increment the value
	newVal := valInt + delta
	err = tx.update(key, newVal)
	if err != nil {
		return 0, err
	}

	return newVal, nil
}

// IncrFloat increments the key value by the specified amount.
// If the key does not exist, sets it to 0 before the increment.
// Returns the value after the increment.
// Returns an error if the key value is not a float.
func (tx *Tx) IncrFloat(key string, delta float64) (float64, error) {
	// get the current value
	val, err := tx.Get(key)
	if err != nil {
		return 0, err
	}

	// check if the value is a valid float
	valFloat, err := val.Float()
	if err != nil {
		return 0, core.ErrValueType
	}

	// increment the value
	newVal := valFloat + delta
	err = tx.update(key, newVal)
	if err != nil {
		return 0, err
	}

	return newVal, nil
}

// Set sets the key value that will not expire.
// Overwrites the value if the key already exists.
func (tx *Tx) Set(key string, value any) error {
	return tx.SetExpires(key, value, 0)
}

// SetExists sets the key value if the key exists.
// Optionally sets the expiration time (if ttl > 0).
// Returns true if the key was set, false if the key does not exist.
func (tx *Tx) SetExists(key string, value any, ttl time.Duration) (bool, error) {
	if !core.IsValueType(value) {
		return false, core.ErrValueType
	}

	k, err := rkey.Get(tx.tx, key)
	if err != nil {
		return false, err
	}
	if !k.Exists() {
		return false, nil
	}

	err = tx.set(key, value, ttl)
	return err == nil, err
}

// SetExpires sets the key value with an optional expiration time (if ttl > 0).
// Overwrites the value and ttl if the key already exists.
func (tx *Tx) SetExpires(key string, value any, ttl time.Duration) error {
	if !core.IsValueType(value) {
		return core.ErrValueType
	}
	err := tx.set(key, value, ttl)
	return err
}

// SetMany sets the values of multiple keys.
// Overwrites values for keys that already exist and
// creates new keys/values for keys that do not exist.
// Removes the TTL for existing keys.
func (tx *Tx) SetMany(items map[string]any) error {
	for _, val := range items {
		if !core.IsValueType(val) {
			return core.ErrValueType
		}
	}

	for key, val := range items {
		err := tx.set(key, val, 0)
		if err != nil {
			return err
		}
	}

	return nil
}

// SetManyNX sets the values of multiple keys, but only if none
// of them yet exist. Returns true if the keys were set, false if any
// of them already exist.
func (tx *Tx) SetManyNX(items map[string]any) (bool, error) {
	for _, val := range items {
		if !core.IsValueType(val) {
			return false, core.ErrValueType
		}
	}

	// extract keys
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}

	// check if any of the keys exist
	count, err := rkey.Count(tx.tx, keys...)
	if err != nil {
		return false, err
	}

	// do not proceed if any of the keys exist
	if count != 0 {
		return false, nil
	}

	// set the keys
	for key, val := range items {
		err = tx.set(key, val, 0)
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

// SetNotExists sets the key value if the key does not exist.
// Optionally sets the expiration time (if ttl > 0).
// Returns true if the key was set, false if the key already exists.
func (tx *Tx) SetNotExists(key string, value any, ttl time.Duration) (bool, error) {
	if !core.IsValueType(value) {
		return false, core.ErrValueType
	}

	k, err := rkey.Get(tx.tx, key)
	if err != nil {
		return false, err
	}
	if k.Exists() {
		return false, nil
	}

	err = tx.set(key, value, ttl)
	return err == nil, err
}

// set sets the key value and (optionally) its expiration time.
func (tx *Tx) set(key string, value any, ttl time.Duration) error {
	now := time.Now()
	var etime *int64
	if ttl > 0 {
		etime = new(int64)
		*etime = now.Add(ttl).UnixMilli()
	}

	args := []any{
		sql.Named("key", key),
		sql.Named("type", core.TypeString),
		sql.Named("version", core.InitialVersion),
		sql.Named("value", value),
		sql.Named("etime", etime),
		sql.Named("mtime", now.UnixMilli()),
	}

	_, err := tx.tx.Exec(sqlSet1, args...)
	if err != nil {
		return sqlx.TypedError(err)
	}

	_, err = tx.tx.Exec(sqlSet2, args...)
	return err
}

// update updates the value of the existing key without changing its
// expiration time. If the key does not exist, creates a new key with
// the specified value and no expiration time.
func (tx *Tx) update(key string, value any) error {
	now := time.Now().UnixMilli()
	args := []any{
		sql.Named("key", key),
		sql.Named("type", core.TypeString),
		sql.Named("version", core.InitialVersion),
		sql.Named("value", value),
		sql.Named("mtime", now),
	}
	_, err := tx.tx.Exec(sqlUpdate1, args...)
	if err != nil {
		return sqlx.TypedError(err)
	}
	_, err = tx.tx.Exec(sqlUpdate2, args...)
	return err
}

// scanValue scans a key value from the row (rows).
func scanValue(scanner sqlx.RowScanner) (key string, val core.Value, err error) {
	var value []byte
	err = scanner.Scan(&key, &value)
	if err == sql.ErrNoRows {
		return "", nil, nil
	}
	if err != nil {
		return "", nil, err
	}
	return key, core.Value(value), nil
}
