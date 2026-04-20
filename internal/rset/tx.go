package rset

import (
	"database/sql"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
	"github.com/nalgeon/redka/internal/storage"
)

// scanPageSize is the default number
// of set items per page when scanning.
const scanPageSize = 10

// Tx is a set repository transaction.
type Tx struct {
	dialect sqlx.Dialect
	tx      sqlx.Tx
	s       storage.SetStorage
}

// NewTx creates a set repository transaction
// from a generic database transaction.
func NewTx(dialect sqlx.Dialect, st storage.Storage, tx sqlx.Tx) *Tx {
	return &Tx{dialect: dialect, tx: tx, s: st.Sets()}
}

// Add adds or updates elements in a set.
func (tx *Tx) Add(key string, elems ...any) (int, error) {
	elembs, err := core.ToBytesMany(elems...)
	if err != nil {
		return 0, err
	}
	keyID, err := tx.s.EnsureSetKey(tx.tx, key, time.Now().UnixMilli())
	if err != nil {
		return 0, tx.dialect.TypedError(err)
	}
	var n int
	for _, elemb := range elembs {
		created, err := tx.s.InsertSetElem(tx.tx, keyID, elemb)
		if err != nil {
			return 0, err
		}
		if created {
			n++
		}
	}
	return n, nil
}

// Delete removes elements from a set.
func (tx *Tx) Delete(key string, elems ...any) (int, error) {
	elembs, err := core.ToBytesMany(elems...)
	if err != nil {
		return 0, err
	}
	return tx.s.DeleteElems(tx.tx, key, time.Now().UnixMilli(), elembs)
}

// Diff returns the difference between the first set and the rest.
func (tx *Tx) Diff(keys ...string) ([]core.Value, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	others := keys[1:]
	if len(others) == 0 {
		return tx.Items(keys[0])
	}
	raw, err := tx.s.DiffElems(tx.tx, keys[0], time.Now().UnixMilli(), others)
	if err != nil {
		return nil, err
	}
	return bytesToVals(raw), nil
}

// DiffStore calculates the difference and stores the result in dest.
func (tx *Tx) DiffStore(dest string, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	now := time.Now().UnixMilli()
	if err := tx.deleteKey(dest, now); err != nil {
		return 0, err
	}
	destID, err := tx.createKey(dest, now)
	if err != nil {
		return 0, err
	}
	others := keys[1:]
	if len(others) == 0 {
		return tx.s.CloneElems(tx.tx, destID, keys[0], now)
	}
	return tx.s.DiffStore(tx.tx, destID, keys[0], others, now)
}

// Exists reports whether the element belongs to a set.
func (tx *Tx) Exists(key, elem any) (bool, error) {
	keyStr, ok := key.(string)
	if !ok {
		return false, core.ErrArgument
	}
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return false, err
	}
	return tx.s.Exists(tx.tx, keyStr, time.Now().UnixMilli(), elemb)
}

// Inter returns the intersection of multiple sets.
func (tx *Tx) Inter(keys ...string) ([]core.Value, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	raw, err := tx.s.InterElems(tx.tx, time.Now().UnixMilli(), keys)
	if err != nil {
		return nil, err
	}
	return bytesToVals(raw), nil
}

// InterStore intersects multiple sets and stores the result in dest.
func (tx *Tx) InterStore(dest string, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	now := time.Now().UnixMilli()
	if err := tx.deleteKey(dest, now); err != nil {
		return 0, err
	}
	destID, err := tx.createKey(dest, now)
	if err != nil {
		return 0, err
	}
	return tx.s.InterStore(tx.tx, destID, keys, now)
}

// Items returns all elements in a set.
func (tx *Tx) Items(key string) ([]core.Value, error) {
	raw, err := tx.s.Items(tx.tx, key, time.Now().UnixMilli())
	if err != nil {
		return nil, err
	}
	return bytesToVals(raw), nil
}

// Len returns the number of elements in a set.
func (tx *Tx) Len(key string) (int, error) {
	return tx.s.Len(tx.tx, key, time.Now().UnixMilli())
}

// Move moves an element from one set to another.
func (tx *Tx) Move(src, dest string, elem any) error {
	n, err := tx.Delete(src, elem)
	if err != nil {
		return err
	}
	if n == 0 {
		return core.ErrNotFound
	}
	_, err = tx.Add(dest, elem)
	return err
}

// Pop removes and returns a random element from a set.
func (tx *Tx) Pop(key string) (core.Value, error) {
	now := time.Now().UnixMilli()
	val, err := tx.s.Pop(tx.tx, key, now)
	if err == sql.ErrNoRows {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := tx.s.PopDecr(tx.tx, key, now); err != nil {
		return nil, err
	}
	return core.Value(val), nil
}

// Random returns a random element from a set.
func (tx *Tx) Random(key string) (core.Value, error) {
	val, err := tx.s.Random(tx.tx, key, time.Now().UnixMilli())
	if err == sql.ErrNoRows {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return core.Value(val), nil
}

// Scan iterates over set elements matching pattern.
func (tx *Tx) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	if count == 0 {
		count = scanPageSize
	}
	rows, err := tx.s.Scan(tx.tx, key, time.Now().UnixMilli(), cursor, pattern, count)
	if err != nil {
		return ScanResult{}, err
	}
	maxID := 0
	elems := make([]core.Value, 0, len(rows))
	for _, r := range rows {
		elems = append(elems, core.Value(r.Elem))
		if r.RowID > maxID {
			maxID = r.RowID
		}
	}
	return ScanResult{maxID, elems}, nil
}

// Scanner returns an iterator over set elements matching pattern.
func (tx *Tx) Scanner(key, pattern string, pageSize int) *Scanner {
	return newScanner(tx, key, pattern, pageSize)
}

// Union returns the union of multiple sets.
func (tx *Tx) Union(keys ...string) ([]core.Value, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	raw, err := tx.s.UnionElems(tx.tx, time.Now().UnixMilli(), keys)
	if err != nil {
		return nil, err
	}
	return bytesToVals(raw), nil
}

// UnionStore unions multiple sets and stores the result in a destination set.
func (tx *Tx) UnionStore(dest string, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	now := time.Now().UnixMilli()
	if err := tx.deleteKey(dest, now); err != nil {
		return 0, err
	}
	destID, err := tx.createKey(dest, now)
	if err != nil {
		return 0, err
	}
	return tx.s.UnionStore(tx.tx, destID, keys, now)
}

func (tx *Tx) deleteKey(key string, now int64) error {
	if err := tx.s.DeleteKeyPart1(tx.tx, key, now); err != nil {
		return err
	}
	return tx.s.DeleteKeyPart2(tx.tx, key, now)
}

func (tx *Tx) createKey(key string, now int64) (int, error) {
	id, err := tx.s.AddKey(tx.tx, key, now)
	if err != nil {
		return 0, tx.dialect.TypedError(err)
	}
	return id, nil
}

func bytesToVals(raw [][]byte) []core.Value {
	if len(raw) == 0 {
		return nil
	}
	out := make([]core.Value, len(raw))
	for i, b := range raw {
		out[i] = core.Value(b)
	}
	return out
}

// ScanResult is a result of the scan operation.
type ScanResult struct {
	Cursor int
	Items  []core.Value
}
