package rlist

import (
	"database/sql"
	"strings"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

// SQL queries for the list repository.
type queries struct {
	delete       string
	deleteBack   string
	deleteFront  string
	get          string
	insert       string
	insertAfter  string
	insertBefore string
	len          string
	popBack      string
	popFront     string
	push         string
	pushBack     string
	pushFront    string
	lrange       string
	set          string
	trim         string
}

// Tx is a list repository transaction.
type Tx struct {
	dialect sqlx.Dialect
	tx      sqlx.Tx
	sql     *queries
}

// NewTx creates a list repository transaction
// from a generic database transaction.
func NewTx(dialect sqlx.Dialect, tx sqlx.Tx) *Tx {
	sql := getSQL(dialect)
	return &Tx{dialect: dialect, tx: tx, sql: sql}
}

// Delete deletes all occurrences of an element from a list.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (tx *Tx) Delete(key string, elem any) (int, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	args := []any{key, time.Now().UnixMilli(), elemb}
	res, err := tx.tx.Exec(tx.sql.delete, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// DeleteBack deletes the first count occurrences of an element
// from a list, starting from the back. Count must be positive.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (tx *Tx) DeleteBack(key string, elem any, count int) (int, error) {
	return tx.delete(key, elem, count, tx.sql.deleteBack)
}

// DeleteFront deletes the first count occurrences of an element
// from a list, starting from the front. Count must be positive.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (tx *Tx) DeleteFront(key string, elem any, count int) (int, error) {
	return tx.delete(key, elem, count, tx.sql.deleteFront)
}

// Get returns an element from a list by index (0-based).
// Negative index count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
// If the index is out of bounds, returns ErrNotFound.
// If the key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) Get(key string, idx int) (core.Value, error) {
	var query = tx.sql.get
	if idx < 0 {
		// Reverse the query ordering and index, e.g.:
		//  - [11 12 13 14], idx = -1 <-> [14 13 12 11], idx = 0 (14)
		//  - [11 12 13 14], idx = -2 <-> [14 13 12 11], idx = 1 (13)
		//  - [11 12 13 14], idx = -3 <-> [14 13 12 11], idx = 2 (12)
		//  - etc.
		query = strings.Replace(query, sqlx.Asc, sqlx.Desc, 1)
		idx = -idx - 1
	}

	var val []byte
	args := []any{key, time.Now().UnixMilli(), idx}
	err := tx.tx.QueryRow(query, args...).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return core.Value(val), nil
}

// InsertAfter inserts an element after another element (pivot).
// Returns the length of the list after the operation.
// If the pivot does not exist, returns (-1, ErrNotFound).
// If the key does not exist or is not a list, returns (0, ErrNotFound).
func (tx *Tx) InsertAfter(key string, pivot, elem any) (int, error) {
	return tx.insert(key, pivot, elem, tx.sql.insertAfter)
}

// InsertBefore inserts an element before another element (pivot).
// Returns the length of the list after the operation.
// If the pivot does not exist, returns (-1, ErrNotFound).
// If the key does not exist or is not a list, returns (0, ErrNotFound).
func (tx *Tx) InsertBefore(key string, pivot, elem any) (int, error) {
	return tx.insert(key, pivot, elem, tx.sql.insertBefore)
}

// Len returns the number of elements in a list.
// If the key does not exist or is not a list, returns 0.
func (tx *Tx) Len(key string) (int, error) {
	var count int
	args := []any{key, time.Now().UnixMilli()}
	err := tx.tx.QueryRow(tx.sql.len, args...).Scan(&count)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return count, nil
}

// PopBack removes and returns the last element of a list.
// If the key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) PopBack(key string) (core.Value, error) {
	return tx.pop(key, tx.sql.popBack)
}

// PopBackPushFront removes the last element of a list
// and prepends it to another list (or the same list).
// If the source key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) PopBackPushFront(src, dest string) (core.Value, error) {
	// Pop the last element from the source list.
	elem, err := tx.PopBack(src)
	if err == core.ErrNotFound {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// Prepend the element to the destination list.
	_, err = tx.PushFront(dest, elem.Bytes())
	return elem, err
}

// PopFront removes and returns the first element of a list.
// If the key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) PopFront(key string) (core.Value, error) {
	return tx.pop(key, tx.sql.popFront)
}

// PushBack appends an element to a list.
// Returns the length of the list after the operation.
// If the key does not exist, creates it.
// If the key exists but is not a list, returns ErrKeyType.
func (tx *Tx) PushBack(key string, elem any) (int, error) {
	return tx.push(key, elem, tx.sql.pushBack)
}

// PushFront prepends an element to a list.
// Returns the length of the list after the operation.
// If the key does not exist, creates it.
// If the key exists but is not a list, returns ErrKeyType.
func (tx *Tx) PushFront(key string, elem any) (int, error) {
	return tx.push(key, elem, tx.sql.pushFront)
}

// Range returns a range of elements from a list.
// Both start and stop are zero-based, inclusive.
// Negative indexes count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
// If the key does not exist or is not a list, returns an empty slice.
func (tx *Tx) Range(key string, start, stop int) ([]core.Value, error) {
	if (start > stop) && (start > 0 && stop > 0 || start < 0 && stop < 0) {
		return nil, nil
	}

	args := []any{
		key, time.Now().UnixMilli(),
		start, start, start,
		stop, stop, stop,
	}
	rows, err := tx.tx.Query(tx.sql.lrange, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var values []core.Value
	for rows.Next() {
		var val []byte
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}
		values = append(values, core.Value(val))
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

// Set sets an element in a list by index (0-based).
// Negative index count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
// If the index is out of bounds, returns ErrNotFound.
// If the key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) Set(key string, idx int, elem any) error {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return err
	}

	var query = tx.sql.set
	if idx < 0 {
		// Reverse the query ordering and index.
		query = strings.Replace(query, sqlx.Asc, sqlx.Desc, 1)
		idx = -idx - 1
	}

	args := []any{key, time.Now().UnixMilli(), elemb, idx}
	out, err := tx.tx.Exec(query, args...)
	if err != nil {
		return err
	}
	n, _ := out.RowsAffected()
	if n == 0 {
		return core.ErrNotFound
	}
	return err
}

// Trim removes elements from both ends of a list so that
// only the elements between start and stop indexes remain.
// Returns the number of elements removed.
//
// Both start and stop are zero-based, inclusive.
// Negative indexes count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
//
// Does nothing if the key does not exist or is not a list.
func (tx *Tx) Trim(key string, start, stop int) (int, error) {
	args := []any{
		key, time.Now().UnixMilli(),
		start, start, start,
		stop, stop, stop,
	}
	out, err := tx.tx.Exec(tx.sql.trim, args...)
	if err != nil {
		return 0, err
	}
	n, _ := out.RowsAffected()
	return int(n), nil
}

// delete removes the first count occurrences of an element
// from a list, starting from the front or back.
func (tx *Tx) delete(key string, elem any, count int, query string) (int, error) {
	if count <= 0 {
		return 0, nil
	}
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}

	args := []any{key, time.Now().UnixMilli(), elemb, count}
	res, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// insert inserts an element before or after a pivot in a list.
func (tx *Tx) insert(key string, pivot, elem any, query string) (int, error) {
	now := time.Now().UnixMilli()
	pivotb, err := core.ToBytes(pivot)
	if err != nil {
		return 0, err
	}
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}

	// Update the key.
	var keyID, n int
	args := []any{now, key, now}
	err = tx.tx.QueryRow(tx.sql.insert, args...).Scan(&keyID, &n)
	if err == sql.ErrNoRows {
		return 0, core.ErrNotFound
	}
	if err != nil {
		return 0, err
	}

	// Insert the element.
	args = []any{keyID, pivotb, keyID, keyID, elemb, keyID}
	_, err = tx.tx.Exec(query, args...)
	if err != nil {
		if tx.dialect.ConstraintFailed(err, "not null", "rlist", "pos") {
			return -1, core.ErrNotFound
		}
		return 0, err
	}

	return n, nil
}

// pop removes and returns an element from the front or back of a list.
func (tx *Tx) pop(key string, query string) (core.Value, error) {
	var val []byte
	args := []any{key, time.Now().UnixMilli()}
	err := tx.tx.QueryRow(query, args...).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return core.Value(val), nil
}

// push inserts an element to the front or back of a list.
func (tx *Tx) push(key string, elem any, query string) (int, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}

	// Create or update the key.
	args := []any{key, time.Now().UnixMilli()}
	var keyID, n int
	err = tx.tx.QueryRow(tx.sql.push, args...).Scan(&keyID, &n)
	if err != nil {
		return 0, tx.dialect.TypedError(err)
	}

	// Insert the element.
	args = []any{keyID, elemb, keyID}
	_, err = tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	return n, nil
}

// getSQL returns the SQL queries for the specified dialect.
func getSQL(dialect sqlx.Dialect) *queries {
	switch dialect {
	case sqlx.DialectSqlite:
		return &sqlite
	case sqlx.DialectPostgres:
		return &postgres
	default:
		return &queries{}
	}
}
