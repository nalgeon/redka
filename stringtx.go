// Redis-like string repository in SQLite.
package redka

import (
	"database/sql"
	"slices"
	"time"
)

const sqlStringGet = `
select key, value
from rstring
join rkey on key_id = rkey.id
where key = ? and (etime is null or etime > ?);
`

const sqlStringLen = `
select length(value)
from rstring
where key_id = (
  select id from rkey
  where key = ? and (etime is null or etime > ?)
);
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

// StringTx is a string repository transaction.
type StringTx struct {
	tx sqlTx
}

// newStringTx creates a string repository transaction
// from a generic database transaction.
func newStringTx(tx sqlTx) *StringTx {
	return &StringTx{tx}
}

// Get returns the value of the key.
func (tx *StringTx) Get(key string) (Value, error) {
	now := time.Now().UnixMilli()
	row := tx.tx.QueryRow(sqlStringGet, key, now)
	_, val, err := scanValue(row)
	return val, err
}

// GetMany returns the values of multiple keys.
func (tx *StringTx) GetMany(keys ...string) ([]Value, error) {
	now := time.Now().UnixMilli()
	query, keyArgs := sqlExpandIn(sqlStringGetMany, ":keys", keys)
	args := slices.Concat(keyArgs, []any{sql.Named("now", now)})

	var rows *sql.Rows
	rows, err := tx.tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Build a map of known keys.
	// It will be used to fill in the missing keys.
	known := make(map[string]Value, len(keys))
	for rows.Next() {
		key, val, err := scanValue(rows)
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
	vals := make([]Value, 0, len(keys))
	for _, key := range keys {
		vals = append(vals, known[key])
	}

	return vals, nil
}

// Set sets the key value. The key does not expire.
func (tx *StringTx) Set(key string, value any) error {
	return tx.SetExpires(key, value, 0)
}

// SetExpires sets the key value with an optional expiration time (if ttl > 0).
func (tx *StringTx) SetExpires(key string, value any, ttl time.Duration) error {
	if !isValueType(value) {
		return ErrInvalidType
	}
	err := tx.set(key, value, ttl)
	return err
}

// SetNotExists sets the key value if the key does not exist.
// Optionally sets the expiration time (if ttl > 0).
func (tx *StringTx) SetNotExists(key string, value any, ttl time.Duration) (bool, error) {
	if !isValueType(value) {
		return false, ErrInvalidType
	}

	k, err := txKeyGet(tx.tx, key)
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
func (tx *StringTx) SetExists(key string, value any, ttl time.Duration) (bool, error) {
	if !isValueType(value) {
		return false, ErrInvalidType
	}

	k, err := txKeyGet(tx.tx, key)
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
func (tx *StringTx) GetSet(key string, value any, ttl time.Duration) (Value, error) {
	if !isValueType(value) {
		return nil, ErrInvalidType
	}

	prev, err := tx.Get(key)
	if err != nil {
		return nil, err
	}

	err = tx.set(key, value, ttl)
	return prev, err
}

// SetMany sets the values of multiple keys.
func (tx *StringTx) SetMany(kvals ...KeyValue) error {
	for _, kv := range kvals {
		if !isValueType(kv.Value) {
			return ErrInvalidType
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
func (tx *StringTx) SetManyNX(kvals ...KeyValue) (bool, error) {
	for _, kv := range kvals {
		if !isValueType(kv.Value) {
			return false, ErrInvalidType
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
	query, keyArgs := sqlExpandIn(sqlKeyCount, ":keys", keys)
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

// Length returns the length of the key value.
func (tx *StringTx) Length(key string) (int, error) {
	now := time.Now().UnixMilli()
	var n int
	// err := tx.tx.Get(&n, sqlStringLen, key, now)
	err := tx.tx.QueryRow(sqlStringLen, key, now).Scan(&n)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return n, err
}

// GetRange returns the substring of the key value.
func (tx *StringTx) GetRange(key string, start, end int) (Value, error) {
	val, err := tx.Get(key)
	if err != nil {
		return nil, err
	}
	if val.IsEmpty() {
		// return empty value if the key does not exist
		return val, nil
	}
	s := val.String()
	start, end = rangeToSlice(len(s), start, end)
	return Value(s[start:end]), nil
}

// SetRange overwrites part of the key value.
func (tx *StringTx) SetRange(key string, offset int, value string) (int, error) {
	val, err := tx.Get(key)
	if err != nil {
		return 0, err
	}

	newVal := setRange(val.String(), offset, value)
	err = tx.update(key, newVal)
	if err != nil {
		return 0, err
	}

	return len(newVal), nil
}

// Append appends the value to the key.
func (tx *StringTx) Append(key, value string) (int, error) {
	val, err := tx.Get(key)
	if err != nil {
		return 0, err
	}

	newVal := val.String() + value
	err = tx.update(key, newVal)
	if err != nil {
		return 0, err
	}

	return len(newVal), nil
}

// Incr increments the key value by the specified amount.
func (tx *StringTx) Incr(key string, delta int) (int, error) {
	// get the current value
	val, err := tx.Get(key)
	if err != nil {
		return 0, err
	}

	// check if the value is a valid integer
	isFound := !val.IsEmpty()
	valInt, err := val.Int()
	if isFound && err != nil {
		return 0, ErrInvalidInt
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
func (tx *StringTx) IncrFloat(key string, delta float64) (float64, error) {
	// get the current value
	val, err := tx.Get(key)
	if err != nil {
		return 0, err
	}

	// check if the value is a valid float
	isFound := !val.IsEmpty()
	valFloat, err := val.Float()
	if isFound && err != nil {
		return 0, ErrInvalidFloat
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
func (tx *StringTx) Delete(keys ...string) (int, error) {
	return txKeyDelete(tx.tx, keys...)
}

// set sets the key value and (optionally) its expiration time.
func (tx StringTx) set(key string, value any, ttl time.Duration) error {
	now := time.Now()
	var etime *int64
	if ttl > 0 {
		etime = new(int64)
		*etime = now.Add(ttl).UnixMilli()
	}

	args := []any{
		sql.Named("key", key),
		sql.Named("type", typeString),
		sql.Named("version", initialVersion),
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
func (tx StringTx) update(key string, value any) error {
	now := time.Now().UnixMilli()
	args := []any{
		sql.Named("key", key),
		sql.Named("type", typeString),
		sql.Named("version", initialVersion),
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
