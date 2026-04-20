package rhash

import (
	"database/sql"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
	"github.com/nalgeon/redka/internal/storage"
)

// scanPageSize is the default number
// of hash items per page when scanning.
const scanPageSize = 10

// Tx is a hash repository transaction.
type Tx struct {
	dialect sqlx.Dialect
	tx      sqlx.Tx
	h       storage.HashStorage
}

// NewTx creates a hash repository transaction
// from a generic database transaction.
func NewTx(dialect sqlx.Dialect, st storage.Storage, tx sqlx.Tx) *Tx {
	return &Tx{dialect: dialect, tx: tx, h: st.Hashes()}
}

// Delete deletes one or more items from a hash.
func (tx *Tx) Delete(key string, fields ...string) (int, error) {
	if len(fields) == 0 {
		return 0, core.ErrArgument
	}
	return tx.h.DeleteFields(tx.tx, key, time.Now().UnixMilli(), fields)
}

// Exists checks if a field exists in a hash.
func (tx *Tx) Exists(key, field string) (bool, error) {
	count, err := tx.count(key, field)
	return count > 0, err
}

// Fields returns all fields in a hash.
func (tx *Tx) Fields(key string) ([]string, error) {
	fields, err := tx.h.Fields(tx.tx, key, time.Now().UnixMilli())
	if err != nil {
		return nil, err
	}
	if fields == nil {
		return []string{}, nil
	}
	return fields, nil
}

// Get returns the value of a field in a hash.
func (tx *Tx) Get(key, field string) (core.Value, error) {
	val, err := tx.h.Get(tx.tx, key, field, time.Now().UnixMilli())
	if err == sql.ErrNoRows {
		return core.Value(nil), core.ErrNotFound
	}
	if err != nil {
		return core.Value(nil), err
	}
	return core.Value(val), nil
}

// GetMany returns a map of values for given fields.
func (tx *Tx) GetMany(key string, fields ...string) (map[string]core.Value, error) {
	rows, err := tx.h.GetMany(tx.tx, key, time.Now().UnixMilli(), fields)
	if err != nil {
		return nil, err
	}
	items := map[string]core.Value{}
	for _, hf := range rows {
		items[hf.Field] = core.Value(hf.Value)
	}
	return items, nil
}

// Incr increments the integer value of a field in a hash.
func (tx *Tx) Incr(key, field string, delta int) (int, error) {
	val, err := tx.Get(key, field)
	if err != nil && err != core.ErrNotFound {
		return 0, err
	}
	valInt, err := val.Int()
	if err != nil {
		return 0, core.ErrValueType
	}
	newVal := valInt + delta
	err = tx.set(key, field, newVal)
	if err != nil {
		return 0, err
	}
	return newVal, nil
}

// IncrFloat increments the float value of a field in a hash.
func (tx *Tx) IncrFloat(key, field string, delta float64) (float64, error) {
	val, err := tx.Get(key, field)
	if err != nil && err != core.ErrNotFound {
		return 0, err
	}
	valFloat, err := val.Float()
	if err != nil {
		return 0, core.ErrValueType
	}
	newVal := valFloat + delta
	err = tx.set(key, field, newVal)
	if err != nil {
		return 0, err
	}
	return newVal, nil
}

// Items returns a map of all fields and values in a hash.
func (tx *Tx) Items(key string) (map[string]core.Value, error) {
	rows, err := tx.h.Items(tx.tx, key, time.Now().UnixMilli())
	if err != nil {
		return nil, err
	}
	items := map[string]core.Value{}
	for _, hf := range rows {
		items[hf.Field] = core.Value(hf.Value)
	}
	return items, nil
}

// Len returns the number of fields in a hash.
func (tx *Tx) Len(key string) (int, error) {
	return tx.h.Len(tx.tx, key, time.Now().UnixMilli())
}

// Scan iterates over hash items with fields matching pattern.
func (tx *Tx) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	if count == 0 {
		count = scanPageSize
	}
	rows, err := tx.h.Scan(tx.tx, key, time.Now().UnixMilli(), cursor, pattern, count)
	if err != nil {
		return ScanResult{}, err
	}
	items := make([]HashItem, 0, len(rows))
	maxID := 0
	for _, r := range rows {
		items = append(items, HashItem{id: r.RowID, Field: r.Field, Value: core.Value(r.Value)})
		if r.RowID > maxID {
			maxID = r.RowID
		}
	}
	return ScanResult{maxID, items}, nil
}

// Scanner returns an iterator for hash items with fields matching pattern.
func (tx *Tx) Scanner(key, pattern string, pageSize int) *Scanner {
	return newScanner(tx, key, pattern, pageSize)
}

// Set creates or updates the value of a field in a hash.
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
func (tx *Tx) SetMany(key string, items map[string]any) (int, error) {
	for _, val := range items {
		if !core.IsValueType(val) {
			return 0, core.ErrValueType
		}
	}
	fields := make([]string, 0, len(items))
	for field := range items {
		fields = append(fields, field)
	}
	existCount, err := tx.count(key, fields...)
	if err != nil {
		return 0, err
	}
	for field, val := range items {
		if err := tx.set(key, field, val); err != nil {
			return 0, err
		}
	}
	return len(items) - existCount, nil
}

// SetNotExists creates the value of a field in a hash if it does not exist.
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
func (tx *Tx) Values(key string) ([]core.Value, error) {
	raw, err := tx.h.Values(tx.tx, key, time.Now().UnixMilli())
	if err != nil {
		return nil, err
	}
	vals := make([]core.Value, len(raw))
	for i, v := range raw {
		vals[i] = core.Value(v)
	}
	return vals, nil
}

func (tx *Tx) count(key string, fields ...string) (int, error) {
	return tx.h.CountFields(tx.tx, key, time.Now().UnixMilli(), fields)
}

func (tx *Tx) set(key string, field string, value any) error {
	valueb, err := core.ToBytes(value)
	if err != nil {
		return err
	}
	keyID, err := tx.h.SetKey(tx.tx, key, time.Now().UnixMilli())
	if err != nil {
		return tx.dialect.TypedError(err)
	}
	return tx.h.SetField(tx.tx, keyID, field, valueb)
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
