// Redis-like string repository in SQLite.
package rstring

import (
	"database/sql"
	"slices"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

const sqlStringGet = `
select key, value
from rstring
join rkey on key_id = rkey.id
where key = ? and (etime is null or etime > ?);
`

const sqlStringGetMany = `
select key, value
from rstring
join rkey on key_id = rkey.id
where key in (:keys) and (etime is null or etime > :now);
`

var sqlStringSet = []string{
	`insert into rkey (key, type, version, etime, mtime)
	values (:key, :type, :version, :etime, :mtime)
	on conflict (key) do update set
	  version = version+1,
	  etime = excluded.etime,
	  mtime = excluded.mtime
	;`,

	`insert into rstring (key_id, value)
	values ((select id from rkey where key = :key), :value)
	on conflict (key_id) do update
	set value = excluded.value;`,
}

var sqlStringUpdate = []string{
	`insert into rkey (key, type, version, etime, mtime)
	values (:key, :type, :version, null, :mtime)
	on conflict (key) do update set
	  version = version+1,
	  -- not changing etime
	  mtime = excluded.mtime
	;`,

	`insert into rstring (key_id, value)
	values ((select id from rkey where key = :key), :value)
	on conflict (key_id) do update
	set value = excluded.value;`,
}

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
func (tx *Tx) Get(key string) (core.Value, error) {
	now := time.Now().UnixMilli()
	row := tx.tx.QueryRow(sqlStringGet, key, now)
	_, val, err := sqlx.ScanValue(row)
	return val, err
}

// GetMany returns the values of multiple keys.
func (tx *Tx) GetMany(keys ...string) ([]core.Value, error) {
	now := time.Now().UnixMilli()
	query, keyArgs := sqlx.ExpandIn(sqlStringGetMany, ":keys", keys)
	args := slices.Concat(keyArgs, []any{sql.Named("now", now)})

	var rows *sql.Rows
	rows, err := tx.tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Build a map of known keys.
	// It will be used to fill in the missing keys.
	known := make(map[string]core.Value, len(keys))
	for rows.Next() {
		key, val, err := sqlx.ScanValue(rows)
		if err != nil {
			return nil, err
		}
		known[key] = val
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	// Build the result slice.
	// It will contain all values in the order of keys.
	// Missing keys will have nil values.
	vals := make([]core.Value, 0, len(keys))
	for _, key := range keys {
		vals = append(vals, known[key])
	}

	return vals, nil
}

// Set sets the key value. The key does not expire.
func (tx *Tx) Set(key string, value any) error {
	return tx.SetExpires(key, value, 0)
}

// SetExpires sets the key value with an optional expiration time (if ttl > 0).
func (tx *Tx) SetExpires(key string, value any, ttl time.Duration) error {
	if !core.IsValueType(value) {
		return core.ErrInvalidType
	}
	err := tx.set(key, value, ttl)
	return err
}

// SetNotExists sets the key value if the key does not exist.
// Optionally sets the expiration time (if ttl > 0).
func (tx *Tx) SetNotExists(key string, value any, ttl time.Duration) (bool, error) {
	if !core.IsValueType(value) {
		return false, core.ErrInvalidType
	}

	k, err := sqlx.GetKey(tx.tx, key)
	if err != nil {
		return false, err
	}
	if k.Exists() {
		return false, nil
	}

	err = tx.set(key, value, ttl)
	return err == nil, err
}

// SetExists sets the key value if the key exists.
// Optionally sets the expiration time (if ttl > 0).
func (tx *Tx) SetExists(key string, value any, ttl time.Duration) (bool, error) {
	if !core.IsValueType(value) {
		return false, core.ErrInvalidType
	}

	k, err := sqlx.GetKey(tx.tx, key)
	if err != nil {
		return false, err
	}
	if !k.Exists() {
		return false, nil
	}

	err = tx.set(key, value, ttl)
	return err == nil, err
}

// GetSet returns the previous value of a key after setting it to a new value.
// Optionally sets the expiration time (if ttl > 0).
func (tx *Tx) GetSet(key string, value any, ttl time.Duration) (core.Value, error) {
	if !core.IsValueType(value) {
		return nil, core.ErrInvalidType
	}

	prev, err := tx.Get(key)
	if err != nil {
		return nil, err
	}

	err = tx.set(key, value, ttl)
	return prev, err
}

// SetMany sets the values of multiple keys.
func (tx *Tx) SetMany(kvals ...core.KVPair) error {
	for _, kv := range kvals {
		if !core.IsValueType(kv.Value) {
			return core.ErrInvalidType
		}
	}

	for _, kv := range kvals {
		err := tx.set(kv.Key, kv.Value, 0)
		if err != nil {
			return err
		}
	}

	return nil
}

// SetManyNX sets the values of multiple keys,
// but only if none of them exist yet.
func (tx *Tx) SetManyNX(kvals ...core.KVPair) (bool, error) {
	for _, kv := range kvals {
		if !core.IsValueType(kv.Value) {
			return false, core.ErrInvalidType
		}
	}

	// extract keys
	keys := make([]string, 0, len(kvals))
	for _, kv := range kvals {
		keys = append(keys, kv.Key)
	}

	// check if any of the keys exist
	count := 0
	now := time.Now().UnixMilli()
	query, keyArgs := sqlx.ExpandIn(sqlx.SQLKeyCount, ":keys", keys)
	args := slices.Concat(keyArgs, []any{sql.Named("now", now)})
	err := tx.tx.QueryRow(query, args...).Scan(&count)
	if err != nil {
		return false, err
	}

	// do not proceed if any of the keys exist
	if count != 0 {
		return false, nil
	}

	// set the keys
	for _, kv := range kvals {
		err = tx.set(kv.Key, kv.Value, 0)
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

// Incr increments the key value by the specified amount.
func (tx *Tx) Incr(key string, delta int) (int, error) {
	// get the current value
	val, err := tx.Get(key)
	if err != nil {
		return 0, err
	}

	// check if the value is a valid integer
	isFound := !val.IsEmpty()
	valInt, err := val.Int()
	if isFound && err != nil {
		return 0, core.ErrInvalidType
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
func (tx *Tx) IncrFloat(key string, delta float64) (float64, error) {
	// get the current value
	val, err := tx.Get(key)
	if err != nil {
		return 0, err
	}

	// check if the value is a valid float
	isFound := !val.IsEmpty()
	valFloat, err := val.Float()
	if isFound && err != nil {
		return 0, core.ErrInvalidType
	}

	// increment the value
	newVal := valFloat + delta
	err = tx.update(key, newVal)
	if err != nil {
		return 0, err
	}

	return newVal, nil
}

// Delete deletes keys and their values.
// Returns the number of deleted keys. Non-existing keys are ignored.
func (tx *Tx) Delete(keys ...string) (int, error) {
	return sqlx.DeleteKey(tx.tx, keys...)
}

// set sets the key value and (optionally) its expiration time.
func (tx Tx) set(key string, value any, ttl time.Duration) error {
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

	_, err := tx.tx.Exec(sqlStringSet[0], args...)
	if err != nil {
		return err
	}

	_, err = tx.tx.Exec(sqlStringSet[1], args...)
	return err
}

// update updates the value of the existing key without changing its
// expiration time. If the key does not exist, creates a new key with
// the specified value and no expiration time.
func (tx Tx) update(key string, value any) error {
	now := time.Now().UnixMilli()
	args := []any{
		sql.Named("key", key),
		sql.Named("type", core.TypeString),
		sql.Named("version", core.InitialVersion),
		sql.Named("value", value),
		sql.Named("mtime", now),
	}
	_, err := tx.tx.Exec(sqlStringUpdate[0], args...)
	if err != nil {
		return err
	}
	_, err = tx.tx.Exec(sqlStringUpdate[1], args...)
	return err
}
