package rhash

import (
	"database/sql"
	"slices"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

const (
	sqlCount = `
	select count(field)
	from rhash
	  join rkey on key_id = rkey.id and (etime is null or etime > :now)
	where key = :key and field in (:fields)`

	sqlDelete = `
	delete from rhash
	where key_id = (
	    select id from rkey where key = :key
	    and (etime is null or etime > :now)
	  ) and field in (:fields)`

	sqlFields = `
	select field
	from rhash
	  join rkey on key_id = rkey.id and (etime is null or etime > :now)
	where key = :key`

	sqlGet = `
	select value
	from rhash
	  join rkey on key_id = rkey.id and (etime is null or etime > :now)
	where key = :key and field = :field`

	sqlGetMany = `
	select field, value
	from rhash
	  join rkey on key_id = rkey.id and (etime is null or etime > :now)
	where key = :key and field in (:fields)`

	sqlItems = `
	select field, value
	from rhash
	  join rkey on key_id = rkey.id and (etime is null or etime > :now)
	where key = :key`

	sqlLen = `
	select count(field)
	from rhash
	  join rkey on key_id = rkey.id and (etime is null or etime > :now)
	where key = :key`

	sqlScan = `
	select rhash.rowid, field, value
	from rhash
	  join rkey on key_id = rkey.id and (etime is null or etime > :now)
	where key = :key and rhash.rowid > :cursor and field glob :pattern
	limit :count`

	sqlSet1 = `
	insert into rkey (key, type, version, mtime)
	values (:key, :type, :version, :mtime)
	on conflict (key) do update set
	  version = version+1,
	  type = excluded.type,
	  mtime = excluded.mtime`

	sqlSet2 = `
	insert into rhash (key_id, field, value)
	values ((select id from rkey where key = :key), :field, :value)
	on conflict (key_id, field) do update
	set value = excluded.value`

	sqlValues = `
	select value
	from rhash
	  join rkey on key_id = rkey.id and (etime is null or etime > :now)
	where key = :key`
)

const scanPageSize = 10

// Tx is a hash repository transaction.
type Tx struct {
	tx sqlx.Tx
}

// NewTx creates a hash repository transaction
// from a generic database transaction.
func NewTx(tx sqlx.Tx) *Tx {
	return &Tx{tx}
}

// Delete deletes one or more items from a hash.
// Returns the number of fields deleted.
// Ignores non-existing fields.
// Does nothing if the key does not exist or is not a hash.
// Does not delete the key if the hash becomes empty.
func (tx *Tx) Delete(key string, fields ...string) (int, error) {
	now := time.Now().UnixMilli()
	query, fieldArgs := sqlx.ExpandIn(sqlDelete, ":fields", fields)
	args := slices.Concat([]any{sql.Named("key", key), sql.Named("now", now)}, fieldArgs)
	res, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	count, _ := res.RowsAffected()
	return int(count), nil
}

// Exists checks if a field exists in a hash.
// If the key does not exist or is not a hash, returns false.
func (tx *Tx) Exists(key, field string) (bool, error) {
	count, err := tx.count(key, field)
	return count > 0, err
}

// Fields returns all fields in a hash.
// If the key does not exist or is not a hash, returns an empty slice.
func (tx *Tx) Fields(key string) ([]string, error) {
	now := time.Now().UnixMilli()
	args := []any{sql.Named("key", key), sql.Named("now", now)}

	// Select hash fields.
	var rows *sql.Rows
	rows, err := tx.tx.Query(sqlFields, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Build a slice of hash fields.
	fields := []string{}
	for rows.Next() {
		var field string
		err := rows.Scan(&field)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return fields, nil
}

// Get returns the value of a field in a hash.
// If the element does not exist, returns ErrNotFound.
// If the key does not exist or is not a hash, returns ErrNotFound.
func (tx *Tx) Get(key, field string) (core.Value, error) {
	args := []any{
		sql.Named("key", key),
		sql.Named("now", time.Now().UnixMilli()),
		sql.Named("field", field),
	}
	var val []byte
	err := tx.tx.QueryRow(sqlGet, args...).Scan(&val)
	if err == sql.ErrNoRows {
		return core.Value(nil), core.ErrNotFound
	}
	if err != nil {
		return core.Value(nil), err
	}
	return core.Value(val), nil
}

// GetMany returns a map of values for given fields.
// Ignores fields that do not exist and do not return them in the map.
// If the key does not exist or is not a hash, returns an empty map.
func (tx *Tx) GetMany(key string, fields ...string) (map[string]core.Value, error) {
	// Get the values of the requested fields.
	now := time.Now().UnixMilli()
	query, fieldArgs := sqlx.ExpandIn(sqlGetMany, ":fields", fields)
	args := slices.Concat([]any{sql.Named("key", key), sql.Named("now", now)}, fieldArgs)

	var rows *sql.Rows
	rows, err := tx.tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Fill the map with the values for existing fields.
	items := map[string]core.Value{}
	for rows.Next() {
		field, val, err := scanValue(rows)
		if err != nil {
			return nil, err
		}
		items[field] = val
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return items, nil
}

// Incr increments the integer value of a field in a hash.
// Returns the value after the increment.
// If the field does not exist, sets it to 0 before the increment.
// If the field value is not an integer, returns ErrValueType.
// If the key does not exist, creates it.
// If the key exists but is not a hash, returns ErrKeyType.
func (tx *Tx) Incr(key, field string, delta int) (int, error) {
	// get the current value
	val, err := tx.Get(key, field)
	if err != nil && err != core.ErrNotFound {
		return 0, err
	}

	// check if the value is a valid integer
	valInt, err := val.Int()
	if err != nil {
		return 0, core.ErrValueType
	}

	// increment the value
	newVal := valInt + delta
	err = tx.set(key, field, newVal)
	if err != nil {
		return 0, err
	}

	return newVal, nil
}

// IncrFloat increments the float value of a field in a hash.
// Returns the value after the increment.
// If the field does not exist, sets it to 0 before the increment.
// If the field value is not a float, returns ErrValueType.
// If the key does not exist, creates it.
// If the key exists but is not a hash, returns ErrKeyType.
func (tx *Tx) IncrFloat(key, field string, delta float64) (float64, error) {
	// get the current value
	val, err := tx.Get(key, field)
	if err != nil && err != core.ErrNotFound {
		return 0, err
	}

	// check if the value is a valid float
	valFloat, err := val.Float()
	if err != nil {
		return 0, core.ErrValueType
	}

	// increment the value
	newVal := valFloat + delta
	err = tx.set(key, field, newVal)
	if err != nil {
		return 0, err
	}

	return newVal, nil
}

// Items returns a map of all fields and values in a hash.
// If the key does not exist or is not a hash, returns an empty map.
func (tx *Tx) Items(key string) (map[string]core.Value, error) {
	now := time.Now().UnixMilli()
	args := []any{sql.Named("key", key), sql.Named("now", now)}

	// Select hash rows.
	var rows *sql.Rows
	rows, err := tx.tx.Query(sqlItems, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Build a map of hash fields and their values.
	items := map[string]core.Value{}
	for rows.Next() {
		field, val, err := scanValue(rows)
		if err != nil {
			return nil, err
		}
		items[field] = val
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return items, nil
}

// Len returns the number of fields in a hash.
// If the key does not exist or is not a hash, returns 0.
func (tx *Tx) Len(key string) (int, error) {
	now := time.Now().UnixMilli()
	args := []any{sql.Named("key", key), sql.Named("now", now)}
	var n int
	err := tx.tx.QueryRow(sqlLen, args...).Scan(&n)
	return n, err
}

// Scan iterates over hash items with fields matching pattern.
// Returns a slice field-value pairs (see [HashItem]) of size count
// based on the current state of the cursor. Returns an empty HashItem
// slice when there are no more items.
// If the key does not exist or is not a hash, returns a nil slice.
// Supports glob-style patterns. Set count = 0 for default page size.
func (tx *Tx) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	if count == 0 {
		count = scanPageSize
	}

	args := []any{
		sql.Named("key", key),
		sql.Named("now", time.Now().UnixMilli()),
		sql.Named("cursor", cursor),
		sql.Named("pattern", pattern),
		sql.Named("count", count),
	}

	// Select hash items matching the pattern.
	scan := func(rows *sql.Rows) (HashItem, error) {
		var it HashItem
		var val []byte
		err := rows.Scan(&it.id, &it.Field, &val)
		it.Value = core.Value(val)
		return it, err
	}
	items, err := sqlx.Select(tx.tx, sqlScan, args, scan)
	if err != nil {
		return ScanResult{}, err
	}

	// Select the maximum ID.
	maxID := 0
	for _, it := range items {
		if it.id > maxID {
			maxID = it.id
		}
	}

	return ScanResult{maxID, items}, nil
}

// Scanner returns an iterator for hash items with fields matching pattern.
// The scanner returns items one by one, fetching them from the database
// in pageSize batches when necessary. Stops when there are no more items
// or an error occurs. If the key does not exist or is not a hash, stops immediately.
// Supports glob-style patterns. Set pageSize = 0 for default page size.
func (tx *Tx) Scanner(key, pattern string, pageSize int) *Scanner {
	return newScanner(tx, key, pattern, pageSize)
}

// Set creates or updates the value of a field in a hash.
// Returns true if the field was created, false if it was updated.
// If the key does not exist, creates it.
// If the key exists but is not a hash, returns ErrKeyType.
func (tx *Tx) Set(key string, field string, value any) (bool, error) {
	if !core.IsValueType(value) {
		return false, core.ErrValueType
	}
	existCount, err := tx.count(key, field)
	if err != nil {
		return false, err
	}
	err = tx.set(key, field, value)
	if err != nil {
		return false, err
	}
	return existCount == 0, nil
}

// SetMany creates or updates the values of multiple fields in a hash.
// Returns the number of fields created (as opposed to updated).
// If the key does not exist, creates it.
// If the key exists but is not a hash, returns ErrKeyType.
func (tx *Tx) SetMany(key string, items map[string]any) (int, error) {
	for _, val := range items {
		if !core.IsValueType(val) {
			return 0, core.ErrValueType
		}
	}

	// Count the number of existing fields.
	fields := make([]string, 0, len(items))
	for field := range items {
		fields = append(fields, field)
	}
	existCount, err := tx.count(key, fields...)
	if err != nil {
		return 0, err
	}

	// Set the values.
	for field, val := range items {
		err := tx.set(key, field, val)
		if err != nil {
			return 0, err
		}
	}

	return len(items) - existCount, nil
}

// SetNotExists creates the value of a field in a hash if it does not exist.
// Returns true if the field was created, false if it already exists.
// If the key does not exist, creates it.
// If the key exists but is not a hash, returns ErrKeyType.
func (tx *Tx) SetNotExists(key, field string, value any) (bool, error) {
	if !core.IsValueType(value) {
		return false, core.ErrValueType
	}
	exist, err := tx.Exists(key, field)
	if err != nil {
		return false, err
	}
	if exist {
		return false, nil
	}
	err = tx.set(key, field, value)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Values returns all values in a hash.
// If the key does not exist or is not a hash, returns an empty slice.
func (tx *Tx) Values(key string) ([]core.Value, error) {
	now := time.Now().UnixMilli()
	args := []any{sql.Named("key", key), sql.Named("now", now)}

	// Select hash values.
	var rows *sql.Rows
	rows, err := tx.tx.Query(sqlValues, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Build a slice of hash values.
	vals := []core.Value{}
	for rows.Next() {
		var value []byte
		err := rows.Scan(&value)
		if err != nil {
			return nil, err
		}
		vals = append(vals, core.Value(value))
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return vals, nil
}

// count returns the number of existing fields in a hash.
func (tx *Tx) count(key string, fields ...string) (int, error) {
	now := time.Now().UnixMilli()
	query, fieldArgs := sqlx.ExpandIn(sqlCount, ":fields", fields)
	args := slices.Concat([]any{sql.Named("key", key), sql.Named("now", now)}, fieldArgs)
	var count int
	err := tx.tx.QueryRow(query, args...).Scan(&count)
	return count, err
}

// set creates or updates the value of a field in a hash.
func (tx *Tx) set(key string, field string, value any) error {
	args := []any{
		sql.Named("key", key),
		sql.Named("type", core.TypeHash),
		sql.Named("version", core.InitialVersion),
		sql.Named("mtime", time.Now().UnixMilli()),
		sql.Named("field", field),
		sql.Named("value", value),
	}

	_, err := tx.tx.Exec(sqlSet1, args...)
	if err != nil {
		return sqlx.TypedError(err)
	}

	_, err = tx.tx.Exec(sqlSet2, args...)
	return err
}

// scanValue scans a hash field value the current row.
func scanValue(rows *sql.Rows) (field string, val core.Value, err error) {
	var value []byte
	err = rows.Scan(&field, &value)
	if err != nil {
		return "", nil, err
	}
	return field, core.Value(value), nil
}

// HashItem represents an item in a hash.
type HashItem struct {
	id    int
	Field string
	Value core.Value
}

// ScanResult represents a result of the scan command.
type ScanResult struct {
	Cursor int
	Items  []HashItem
}
