package rset

import (
	"database/sql"
	"slices"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

// scanPageSize is the default number
// of set items per page when scanning.
const scanPageSize = 10

// SQL queries for the set repository.
type queries struct {
	add1       string
	add2       string
	delete1    string
	delete2    string
	deleteKey1 string
	deleteKey2 string
	diff       string
	diffStore  string
	exists     string
	inter      string
	interStore string
	items      string
	len        string
	pop1       string
	pop2       string
	random     string
	scan       string
	union      string
	unionStore string
}

// Tx is a set repository transaction.
type Tx struct {
	dialect sqlx.Dialect
	tx      sqlx.Tx
	sql     queries
}

// NewTx creates a set repository transaction
// from a generic database transaction.
func NewTx(dialect sqlx.Dialect, tx sqlx.Tx) *Tx {
	sql := getSQL(dialect)
	return &Tx{dialect: dialect, tx: tx, sql: sql}
}

// Add adds or updates elements in a set.
// Returns the number of elements created (as opposed to updated).
// If the key does not exist, creates it.
// If the key exists but is not a set, returns ErrKeyType.
func (tx *Tx) Add(key string, elems ...any) (int, error) {
	// Check the types of the elements.
	elembs, err := core.ToBytesMany(elems...)
	if err != nil {
		return 0, err
	}

	// Create or update the key.
	var keyID int
	err = tx.tx.QueryRow(tx.sql.add1, key, time.Now().UnixMilli()).Scan(&keyID)
	if err != nil {
		return 0, sqlx.TypedError(err)
	}

	// Add the elements.
	var n int
	for _, elemb := range elembs {
		var created bool
		err = tx.tx.QueryRow(tx.sql.add2, keyID, elemb).Scan(&created)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return 0, err
		}
		n++
	}

	return n, nil
}

// Delete removes elements from a set.
// Returns the number of elements removed.
// Ignores the elements that do not exist.
// Does nothing if the key does not exist or is not a set.
func (tx *Tx) Delete(key string, elems ...any) (int, error) {
	// Check the types of the elements.
	elembs, err := core.ToBytesMany(elems...)
	if err != nil {
		return 0, err
	}

	// Remove the elements.
	now := time.Now().UnixMilli()
	query, elemArgs := sqlx.ExpandIn(tx.sql.delete1, ":elems", elembs)
	query = tx.dialect.Enumerate(query)
	args := append([]any{key, now}, elemArgs...)
	res, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return 0, nil
	}

	// Update the key.
	args = []any{now, n, key, now}
	_, err = tx.tx.Exec(tx.sql.delete2, args...)
	if err != nil {
		return 0, err
	}

	return int(n), nil
}

// Diff returns the difference between the first set and the rest.
// The difference consists of elements that are present in the first set
// but not in any of the rest.
// If the first key does not exist or is not a set, returns an empty slice.
// If any of the remaining keys do not exist or are not sets, ignores them.
func (tx *Tx) Diff(keys ...string) ([]core.Value, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	others := keys[1:]
	now := time.Now().UnixMilli()
	query, keyArgs := sqlx.ExpandIn(tx.sql.diff, ":keys", others)
	query = tx.dialect.Enumerate(query)
	args := append(keyArgs, now, keys[0], now)
	return tx.selectElems(query, args)
}

// DiffStore calculates the difference between the first source set
// and the rest, and stores the result in a destination set.
// Returns the number of elements in the destination set.
// If the destination key already exists, it is fully overwritten
// (all old elements are removed and the new ones are inserted).
// If the destination key already exists and is not a set, returns ErrKeyType.
// If the first source key does not exist or is not a set, does nothing,
// except deleting the destination key if it exists.
// If any of the remaining source keys do not exist or are not sets, ignores them.
func (tx *Tx) DiffStore(dest string, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	// Delete the destination key if it exists.
	now := time.Now().UnixMilli()
	err := tx.deleteKey(dest, now)
	if err != nil {
		return 0, err
	}

	// Create the destination key.
	destID, err := tx.createKey(dest, now)
	if err != nil {
		return 0, err
	}

	// Diff the source sets and store the result.
	others := keys[1:]
	query, keyArgs := sqlx.ExpandIn(tx.sql.diffStore, ":keys", others)
	query = tx.dialect.Enumerate(query)
	args := append(keyArgs, now, destID, keys[0], now)
	return tx.store(query, args)
}

// Exists reports whether the element belongs to a set.
// If the key does not exist or is not a set, returns false.
func (tx *Tx) Exists(key, elem any) (bool, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return false, err
	}

	var exists bool
	args := []any{key, time.Now().UnixMilli(), elemb}
	err = tx.tx.QueryRow(tx.sql.exists, args...).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// Inter returns the intersection of multiple sets.
// The intersection consists of elements that exist in all given sets.
// If any of the source keys do not exist or are not sets,
// returns an empty slice.
func (tx *Tx) Inter(keys ...string) ([]core.Value, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	query, keyArgs := sqlx.ExpandIn(tx.sql.inter, ":keys", keys)
	query = tx.dialect.Enumerate(query)
	args := append(keyArgs, time.Now().UnixMilli(), len(keys))
	return tx.selectElems(query, args)
}

// InterStore intersects multiple sets and stores the result in a destination set.
// Returns the number of elements in the destination set.
// If the destination key already exists, it is fully overwritten
// (all old elements are removed and the new ones are inserted).
// If the destination key already exists and is not a set, returns ErrKeyType.
// If any of the source keys do not exist or are not sets, does nothing,
// except deleting the destination key if it exists.
func (tx *Tx) InterStore(dest string, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	// Delete the destination key if it exists.
	now := time.Now().UnixMilli()
	err := tx.deleteKey(dest, now)
	if err != nil {
		return 0, err
	}

	// Create the destination key.
	destID, err := tx.createKey(dest, now)
	if err != nil {
		return 0, err
	}

	// Intersect the source sets and store the result.
	query, keyArgs := sqlx.ExpandIn(tx.sql.interStore, ":keys", keys)
	query = tx.dialect.Enumerate(query)
	args := slices.Concat([]any{destID}, keyArgs, []any{now, len(keys)})
	return tx.store(query, args)
}

// Items returns all elements in a set.
// If the key does not exist or is not a set, returns an empty slice.
func (tx *Tx) Items(key string) ([]core.Value, error) {
	args := []any{key, time.Now().UnixMilli()}
	return tx.selectElems(tx.sql.items, args)
}

// Len returns the number of elements in a set.
// Returns 0 if the key does not exist or is not a set.
func (tx *Tx) Len(key string) (int, error) {
	var n int
	args := []any{key, time.Now().UnixMilli()}
	err := tx.tx.QueryRow(tx.sql.len, args...).Scan(&n)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return n, err
}

// Move moves an element from one set to another.
// If the element does not exist in the source set, returns ErrNotFound.
// If the source key does not exist or is not a set, returns ErrNotFound.
// If the destination key does not exist, creates it.
// If the destination key exists but is not a set, returns ErrKeyType.
// If the element already exists in the destination set,
// only deletes it from the source set.
func (tx *Tx) Move(src, dest string, elem any) error {
	// Delete the element from the source set.
	n, err := tx.Delete(src, elem)
	if err != nil {
		return err
	}
	if n == 0 {
		return core.ErrNotFound
	}

	// Add the element to the destination set.
	_, err = tx.Add(dest, elem)
	if err != nil {
		return err
	}

	return nil
}

// Pop removes and returns a random element from a set.
// If the key does not exist or is not a set, returns ErrNotFound.
func (tx *Tx) Pop(key string) (core.Value, error) {
	// Pop an element from the set.
	now := time.Now().UnixMilli()
	args := []any{key, now}
	var val []byte
	err := tx.tx.QueryRow(tx.sql.pop1, args...).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// Update the key.
	args = []any{now, 1, key, now}
	_, err = tx.tx.Exec(tx.sql.pop2, args...)
	if err != nil {
		return nil, err
	}

	return core.Value(val), nil
}

// Random returns a random element from a set.
// If the key does not exist or is not a set, returns ErrNotFound.
func (tx *Tx) Random(key string) (core.Value, error) {
	args := []any{key, time.Now().UnixMilli()}
	var val []byte
	err := tx.tx.QueryRow(tx.sql.random, args...).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return core.Value(val), nil
}

// Scan iterates over set elements matching pattern.
// Returns a slice of elements of size count based on the current state
// of the cursor. Returns an empty slice when there are no more items.
// If the key does not exist or is not a set, returns an empty slice.
// Supports glob-style patterns. Set count = 0 for default page size.
func (tx *Tx) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	pattern = tx.dialect.GlobToLike(pattern)
	if count == 0 {
		count = scanPageSize
	}

	// Select set items matching the pattern.
	args := []any{
		key, time.Now().UnixMilli(),
		cursor, pattern, count,
	}
	var rows *sql.Rows
	rows, err := tx.tx.Query(tx.sql.scan, args...)
	if err != nil {
		return ScanResult{}, err
	}
	defer rows.Close()

	// Build the resulting slice.
	maxID := 0
	var elems []core.Value
	for rows.Next() {
		var rowID int
		var val []byte
		err := rows.Scan(&rowID, &val)
		if err != nil {
			return ScanResult{}, err
		}
		elems = append(elems, core.Value(val))
		if rowID > maxID {
			maxID = rowID
		}
	}
	if rows.Err() != nil {
		return ScanResult{}, rows.Err()
	}

	return ScanResult{maxID, elems}, nil
}

// Scanner returns an iterator over set elements matching pattern.
// The scanner returns items one by one, fetching them from the database
// in pageSize batches when necessary. Stops when there are no more items
// or an error occurs. If the key does not exist or is not a set, stops immediately.
// Supports glob-style patterns. Set pageSize = 0 for default page size.
func (tx *Tx) Scanner(key, pattern string, pageSize int) *Scanner {
	return newScanner(tx, key, pattern, pageSize)
}

// Union returns the union of multiple sets.
// The union consists of elements that exist in any of the given sets.
// Ignores the keys that do not exist or are not sets.
// If no keys exist, returns an empty slice.
func (tx *Tx) Union(keys ...string) ([]core.Value, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	query, keyArgs := sqlx.ExpandIn(tx.sql.union, ":keys", keys)
	query = tx.dialect.Enumerate(query)
	args := append(keyArgs, time.Now().UnixMilli())
	return tx.selectElems(query, args)
}

// UnionStore unions multiple sets and stores the result in a destination set.
// Returns the number of elements in the destination set.
// If the destination key already exists, it is fully overwritten
// (all old elements are removed and the new ones are inserted).
// If the destination key already exists and is not a set, returns ErrKeyType.
// Ignores the source keys that do not exist or are not sets.
// If all of the source keys do not exist or are not sets, does nothing,
// except deleting the destination key if it exists.
func (tx *Tx) UnionStore(dest string, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	// Delete the destination key if it exists.
	now := time.Now().UnixMilli()
	err := tx.deleteKey(dest, now)
	if err != nil {
		return 0, err
	}

	// Create the destination key.
	destID, err := tx.createKey(dest, now)
	if err != nil {
		return 0, err
	}

	// Union the source sets and store the result.
	query, keyArgs := sqlx.ExpandIn(tx.sql.unionStore, ":keys", keys)
	query = tx.dialect.Enumerate(query)
	args := slices.Concat([]any{destID}, keyArgs, []any{now})
	return tx.store(query, args)
}

// deleteKey deletes set elements and resets the key metadata.
func (tx *Tx) deleteKey(key string, now int64) error {
	_, err := tx.tx.Exec(tx.sql.deleteKey1, key, now)
	if err != nil {
		return err
	}
	_, err = tx.tx.Exec(tx.sql.deleteKey2, key, now)
	return err
}

// createKey creates a new set key if it does not exist.
func (tx *Tx) createKey(key string, now int64) (int, error) {
	var keyID int
	err := tx.tx.QueryRow(tx.sql.add1, key, now).Scan(&keyID)
	if err != nil {
		return 0, sqlx.TypedError(err)
	}
	return keyID, nil
}

// store executes a set operation and stores the result.
// Returns the number of elements stored.
func (tx *Tx) store(query string, args []any) (int, error) {
	res, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// selectElems selects elements from a set.
func (tx *Tx) selectElems(query string, args []any) ([]core.Value, error) {
	// Execute the query.
	var rows *sql.Rows
	rows, err := tx.tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Build the resulting slice.
	var elems []core.Value
	for rows.Next() {
		var val []byte
		err := rows.Scan(&val)
		if err != nil {
			return nil, err
		}
		elems = append(elems, core.Value(val))
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return elems, nil
}

// ScanResult is a result of the scan operation.
type ScanResult struct {
	Cursor int
	Items  []core.Value
}

// getSQL returns the SQL queries for the specified dialect.
func getSQL(dialect sqlx.Dialect) queries {
	switch dialect {
	case sqlx.DialectSqlite:
		return sqlite
	case sqlx.DialectPostgres:
		return queries{}
	default:
		return queries{}
	}
}
