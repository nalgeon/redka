package rlist

import (
	"database/sql"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
	"github.com/nalgeon/redka/internal/storage"
)

// Tx is a list repository transaction.
type Tx struct {
	dialect sqlx.Dialect
	tx      sqlx.Tx
	l       storage.ListStorage
}

// NewTx creates a list repository transaction
// from a generic database transaction.
func NewTx(dialect sqlx.Dialect, st storage.Storage, tx sqlx.Tx) *Tx {
	return &Tx{dialect: dialect, tx: tx, l: st.Lists()}
}

// Delete deletes all occurrences of an element from a list.
func (tx *Tx) Delete(key string, elem any) (int, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	n, err := tx.l.DeleteEqual(tx.tx, key, elemb, time.Now().UnixMilli())
	return int(n), err
}

// DeleteBack deletes the first count occurrences of an element from the back.
func (tx *Tx) DeleteBack(key string, elem any, count int) (int, error) {
	return tx.deleteDir(key, elem, count, false)
}

// DeleteFront deletes the first count occurrences of an element from the front.
func (tx *Tx) DeleteFront(key string, elem any, count int) (int, error) {
	return tx.deleteDir(key, elem, count, true)
}

func (tx *Tx) deleteDir(key string, elem any, count int, front bool) (int, error) {
	if count <= 0 {
		return 0, nil
	}
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	now := time.Now().UnixMilli()
	var n int64
	if front {
		n, err = tx.l.DeleteFront(tx.tx, key, elemb, now, count)
	} else {
		n, err = tx.l.DeleteBack(tx.tx, key, elemb, now, count)
	}
	return int(n), err
}

// Get returns an element from a list by index (0-based).
func (tx *Tx) Get(key string, idx int) (core.Value, error) {
	indexDesc := idx < 0
	if indexDesc {
		idx = -idx - 1
	}
	val, err := tx.l.Get(tx.tx, key, time.Now().UnixMilli(), idx, indexDesc)
	if err == sql.ErrNoRows {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return core.Value(val), nil
}

// InsertAfter inserts an element after another element (pivot).
func (tx *Tx) InsertAfter(key string, pivot, elem any) (int, error) {
	return tx.insert(key, pivot, elem, true)
}

// InsertBefore inserts an element before another element (pivot).
func (tx *Tx) InsertBefore(key string, pivot, elem any) (int, error) {
	return tx.insert(key, pivot, elem, false)
}

func (tx *Tx) insert(key string, pivot, elem any, after bool) (int, error) {
	now := time.Now().UnixMilli()
	pivotb, err := core.ToBytes(pivot)
	if err != nil {
		return 0, err
	}
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	keyID, n, err := tx.l.InsertBump(tx.tx, now, key, now)
	if err == sql.ErrNoRows {
		return 0, core.ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	var execErr error
	if after {
		execErr = tx.l.InsertAfter(tx.tx, keyID, pivotb, elemb)
	} else {
		execErr = tx.l.InsertBefore(tx.tx, keyID, pivotb, elemb)
	}
	if execErr != nil {
		if tx.dialect.ConstraintFailed(execErr, "not null", "rlist", "pos") {
			return -1, core.ErrNotFound
		}
		return 0, execErr
	}
	return n, nil
}

// Len returns the number of elements in a list.
func (tx *Tx) Len(key string) (int, error) {
	return tx.l.Len(tx.tx, key, time.Now().UnixMilli())
}

// PopBack removes and returns the last element of a list.
func (tx *Tx) PopBack(key string) (core.Value, error) {
	return tx.pop(key, false)
}

// PopBackPushFront removes the last element and prepends it to another list.
func (tx *Tx) PopBackPushFront(src, dest string) (core.Value, error) {
	elem, err := tx.PopBack(src)
	if err == core.ErrNotFound {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	_, err = tx.PushFront(dest, elem.Bytes())
	return elem, err
}

// PopFront removes and returns the first element of a list.
func (tx *Tx) PopFront(key string) (core.Value, error) {
	return tx.pop(key, true)
}

func (tx *Tx) pop(key string, front bool) (core.Value, error) {
	now := time.Now().UnixMilli()
	var val []byte
	var err error
	if front {
		val, err = tx.l.PopFront(tx.tx, key, now)
	} else {
		val, err = tx.l.PopBack(tx.tx, key, now)
	}
	if err == sql.ErrNoRows {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return core.Value(val), nil
}

// PushBack appends an element to a list.
func (tx *Tx) PushBack(key string, elem any) (int, error) {
	return tx.push(key, elem, false)
}

// PushFront prepends an element to a list.
func (tx *Tx) PushFront(key string, elem any) (int, error) {
	return tx.push(key, elem, true)
}

func (tx *Tx) push(key string, elem any, front bool) (int, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	keyID, n, err := tx.l.PushUpsert(tx.tx, key, time.Now().UnixMilli())
	if err != nil {
		return 0, tx.dialect.TypedError(err)
	}
	if front {
		err = tx.l.PushFront(tx.tx, keyID, elemb)
	} else {
		err = tx.l.PushBack(tx.tx, keyID, elemb)
	}
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Range returns a range of elements from a list.
func (tx *Tx) Range(key string, start, stop int) ([]core.Value, error) {
	if (start > stop) && (start > 0 && stop > 0 || start < 0 && stop < 0) {
		return nil, nil
	}
	raw, err := tx.l.Range(tx.tx, key, time.Now().UnixMilli(), start, stop)
	if err != nil {
		return nil, err
	}
	out := make([]core.Value, len(raw))
	for i, b := range raw {
		out[i] = core.Value(b)
	}
	return out, nil
}

// Set sets an element in a list by index (0-based).
func (tx *Tx) Set(key string, idx int, elem any) error {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return err
	}
	indexDesc := idx < 0
	if indexDesc {
		idx = -idx - 1
	}
	n, err := tx.l.Set(tx.tx, key, time.Now().UnixMilli(), elemb, idx, indexDesc)
	if err != nil {
		return err
	}
	if n == 0 {
		return core.ErrNotFound
	}
	return nil
}

// Trim removes elements from both ends of a list.
func (tx *Tx) Trim(key string, start, stop int) (int, error) {
	n, err := tx.l.Trim(tx.tx, key, time.Now().UnixMilli(), start, stop)
	return int(n), err
}
