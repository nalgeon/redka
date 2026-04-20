package rstring

import (
	"database/sql"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
	"github.com/nalgeon/redka/internal/storage"
)

// Tx is a string repository transaction.
type Tx struct {
	dialect sqlx.Dialect
	tx      sqlx.Tx
	str     storage.StringStorage
}

// NewTx creates a string repository transaction
// from a generic database transaction.
func NewTx(dialect sqlx.Dialect, st storage.Storage, tx sqlx.Tx) *Tx {
	return &Tx{dialect: dialect, tx: tx, str: st.Strings()}
}

// Get returns the value of the key.
// If the key does not exist or is not a string, returns ErrNotFound.
func (tx *Tx) Get(key string) (core.Value, error) {
	return tx.get(key)
}

// GetMany returns a map of values for given keys.
// Ignores keys that do not exist or not strings,
// and does not return them in the map.
func (tx *Tx) GetMany(keys ...string) (map[string]core.Value, error) {
	rows, err := tx.str.GetMany(tx.tx, keys, time.Now().UnixMilli())
	if err != nil {
		return nil, err
	}
	items := map[string]core.Value{}
	for _, kv := range rows {
		items[kv.Key] = core.Value(kv.Val)
	}
	return items, nil
}

// Incr increments the integer key value by the specified amount.
func (tx *Tx) Incr(key string, delta int) (int, error) {
	val, err := tx.Get(key)
	if err != nil && err != core.ErrNotFound {
		return 0, err
	}
	valInt, err := val.Int()
	if err != nil {
		return 0, core.ErrValueType
	}
	newVal := valInt + delta
	err = tx.update(key, newVal)
	if err != nil {
		return 0, err
	}
	return newVal, nil
}

// IncrFloat increments the float key value by the specified amount.
func (tx *Tx) IncrFloat(key string, delta float64) (float64, error) {
	val, err := tx.Get(key)
	if err != nil && err != core.ErrNotFound {
		return 0, err
	}
	valFloat, err := val.Float()
	if err != nil {
		return 0, core.ErrValueType
	}
	newVal := valFloat + delta
	err = tx.update(key, newVal)
	if err != nil {
		return 0, err
	}
	return newVal, nil
}

// Set sets the key value that will not expire.
func (tx *Tx) Set(key string, value any) error {
	return tx.SetExpire(key, value, 0)
}

// SetExpire sets the key value with an optional expiration time (if ttl > 0).
func (tx *Tx) SetExpire(key string, value any, ttl time.Duration) error {
	var at time.Time
	if ttl > 0 {
		at = time.Now().Add(ttl)
	}
	return tx.set(key, value, at)
}

// SetMany sets the values of multiple keys.
func (tx *Tx) SetMany(items map[string]any) error {
	for _, val := range items {
		if !core.IsValueType(val) {
			return core.ErrValueType
		}
	}
	at := time.Time{}
	for key, val := range items {
		if err := tx.set(key, val, at); err != nil {
			return err
		}
	}
	return nil
}

// SetWith sets the key value with additional options.
func (tx *Tx) SetWith(key string, value any) SetCmd {
	return SetCmd{tx: tx, key: key, val: value}
}

func (tx *Tx) get(key string) (core.Value, error) {
	val, err := tx.str.Get(tx.tx, key, time.Now().UnixMilli())
	if err == sql.ErrNoRows {
		return core.Value(nil), core.ErrNotFound
	}
	if err != nil {
		return core.Value(nil), err
	}
	return core.Value(val), nil
}

func (tx *Tx) set(key string, value any, at time.Time) error {
	valueb, err := core.ToBytes(value)
	if err != nil {
		return err
	}
	var etime *int64
	if !at.IsZero() {
		etime = new(int64)
		*etime = at.UnixMilli()
	}
	err = tx.str.SetKey(tx.tx, key, etime, time.Now().UnixMilli())
	if err != nil {
		return tx.dialect.TypedError(err)
	}
	return tx.str.SetValue(tx.tx, key, valueb)
}

func (tx *Tx) update(key string, value any) error {
	valueb, err := core.ToBytes(value)
	if err != nil {
		return err
	}
	err = tx.str.UpdateKey(tx.tx, key, time.Now().UnixMilli())
	if err != nil {
		return tx.dialect.TypedError(err)
	}
	return tx.str.UpdateValue(tx.tx, key, valueb)
}
