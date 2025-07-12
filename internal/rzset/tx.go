package rzset

import (
	"database/sql"
	"strings"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

// scanPageSize is the default number
// of set items per page when scanning.
const scanPageSize = 10

// SQL queries for the sorted set repository.
type queries struct {
	add1        string
	add2        string
	count       string
	countScore  string
	delete1     string
	delete2     string
	deleteAll1  string
	deleteAll2  string
	deleteRank  string
	deleteScore string
	getRank     string
	getScore    string
	incr        string
	inter       string
	interStore  string
	len         string
	rangeRank   string
	rangeScore  string
	scan        string
	union       string
	unionStore  string
}

// Tx is a sorted set repository transaction.
type Tx struct {
	dialect sqlx.Dialect
	tx      sqlx.Tx
	sql     *queries
}

// NewTx creates a sorted set repository transaction
// from a generic database transaction.
func NewTx(dialect sqlx.Dialect, tx sqlx.Tx) *Tx {
	sql := getSQL(dialect)
	return &Tx{dialect: dialect, tx: tx, sql: sql}
}

// Add adds or updates an element in a set.
// Returns true if the element was created, false if it was updated.
// If the key does not exist, creates it.
// If the key exists but is not a set, returns ErrKeyType.
func (tx *Tx) Add(key string, elem any, score float64) (bool, error) {
	existCount, err := tx.count(key, elem)
	if err != nil {
		return false, err
	}
	err = tx.add(key, elem, score)
	if err != nil {
		return false, err
	}
	return existCount == 0, nil
}

// AddMany adds or updates multiple elements in a set.
// Returns the number of elements created (as opposed to updated).
// If the key does not exist, creates it.
// If the key exists but is not a set, returns ErrKeyType.
func (tx *Tx) AddMany(key string, items map[any]float64) (int, error) {
	// Count the number of existing elements.
	elems := make([]any, 0, len(items))
	for elem := range items {
		elems = append(elems, elem)
	}
	existCount, err := tx.count(key, elems...)
	if err != nil {
		return 0, err
	}

	// Add the elements.
	for elem, score := range items {
		err := tx.add(key, elem, score)
		if err != nil {
			return 0, err
		}
	}

	return len(items) - existCount, nil
}

// Count returns the number of elements in a set with a score between
// min and max (inclusive). Exclusive ranges are not supported.
// Returns 0 if the key does not exist or is not a set.
func (tx *Tx) Count(key string, min, max float64) (int, error) {
	args := []any{
		key, time.Now().UnixMilli(),
		min, max,
	}
	var n int
	err := tx.tx.QueryRow(tx.sql.countScore, args...).Scan(&n)
	return n, err
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

// DeleteWith removes elements from a set with additional options.
func (tx *Tx) DeleteWith(key string) DeleteCmd {
	return DeleteCmd{tx: tx, key: key}
}

// GetRank returns the rank and score of an element in a set.
// The rank is the 0-based position of the element in the set, ordered
// by score (from low to high), and then by lexicographical order (ascending).
// If the element does not exist, returns ErrNotFound.
// If the key does not exist or is not a set, returns ErrNotFound.
func (tx *Tx) GetRank(key string, elem any) (rank int, score float64, err error) {
	return tx.getRank(key, elem, sqlx.Asc)
}

// GetRankRev returns the rank and score of an element in a set.
// The rank is the 0-based position of the element in the set, ordered
// by score (from high to low), and then by lexicographical order (descending).
// If the element does not exist, returns ErrNotFound.
// If the key does not exist or is not a set, returns ErrNotFound.
func (tx *Tx) GetRankRev(key string, elem any) (rank int, score float64, err error) {
	return tx.getRank(key, elem, sqlx.Desc)
}

// GetScore returns the score of an element in a set.
// If the element does not exist, returns ErrNotFound.
// If the key does not exist or is not a set, returns ErrNotFound.
func (tx *Tx) GetScore(key string, elem any) (float64, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}

	var score float64
	args := []any{key, time.Now().UnixMilli(), elemb}
	row := tx.tx.QueryRow(tx.sql.getScore, args...)
	err = row.Scan(&score)
	if err == sql.ErrNoRows {
		return 0, core.ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return score, nil
}

// Incr increments the score of an element in a set.
// Returns the score after the increment.
// If the element does not exist, adds it and sets the score to 0.0
// before the increment. If the key does not exist, creates it.
// If the key exists but is not a set, returns ErrKeyType.
func (tx *Tx) Incr(key string, elem any, delta float64) (float64, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}

	args := []any{key, time.Now().UnixMilli()}
	var keyID int
	err = tx.tx.QueryRow(tx.sql.add1, args...).Scan(&keyID)
	if err != nil {
		return 0, sqlx.TypedError(err)
	}

	var score float64
	args = []any{keyID, elemb, delta}
	err = tx.tx.QueryRow(tx.sql.incr, args...).Scan(&score)
	if err != nil {
		return 0, err
	}

	return score, nil
}

// Inter returns the intersection of multiple sets.
// The intersection consists of elements that exist in all given sets.
// The score of each element is the sum of its scores in the given sets.
// If any of the source keys do not exist or are not sets, returns an empty slice.
func (tx *Tx) Inter(keys ...string) ([]SetItem, error) {
	cmd := InterCmd{tx: tx, keys: keys, aggregate: sqlx.Sum}
	return cmd.Run()
}

// InterWith intersects multiple sets with additional options.
func (tx *Tx) InterWith(keys ...string) InterCmd {
	return InterCmd{tx: tx, keys: keys, aggregate: sqlx.Sum}
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

// Range returns a range of elements from a set with ranks between start and stop.
// The rank is the 0-based position of the element in the set, ordered
// by score (from low to high), and then by lexicographical order (ascending).
// Start and stop are 0-based, inclusive. Negative values are not supported.
// If the key does not exist or is not a set, returns a nil slice.
func (tx *Tx) Range(key string, start, stop int) ([]SetItem, error) {
	cmd := RangeCmd{tx: tx, key: key, sortDir: sqlx.Asc}
	return cmd.ByRank(start, stop).Run()
}

// RangeWith ranges elements from a set with additional options.
func (tx *Tx) RangeWith(key string) RangeCmd {
	return RangeCmd{tx: tx, key: key, sortDir: sqlx.Asc}
}

// Scan iterates over set items with elements matching pattern.
// Returns a slice of element-score pairs (see [SetItem]) of size count
// based on the current state of the cursor. Returns an empty SetItem
// slice when there are no more items.
// If the key does not exist or is not a set, returns a nil slice.
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
	scan := func(rows *sql.Rows) (SetItem, error) {
		var it SetItem
		var elem []byte
		err := rows.Scan(&it.id, &elem, &it.Score)
		it.Elem = core.Value(elem)
		return it, err
	}
	items, err := sqlx.Select(tx.tx, tx.sql.scan, args, scan)
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

// Scanner returns an iterator for set items with elements matching pattern.
// The scanner returns items one by one, fetching them from the database
// in pageSize batches when necessary. Stops when there are no more items
// or an error occurs. If the key does not exist or is not a set, stops immediately.
// Supports glob-style patterns. Set pageSize = 0 for default page size.
func (tx *Tx) Scanner(key, pattern string, pageSize int) *Scanner {
	return newScanner(tx, key, pattern, pageSize)
}

// Union returns the union of multiple sets.
// The union consists of elements that exist in any of the given sets.
// The score of each element is the sum of its scores in the given sets.
// Ignores the keys that do not exist or are not sets.
// If no keys exist, returns a nil slice.
func (tx *Tx) Union(keys ...string) ([]SetItem, error) {
	cmd := UnionCmd{tx: tx, keys: keys, aggregate: sqlx.Sum}
	return cmd.Run()
}

// UnionWith unions multiple sets with additional options.
func (tx *Tx) UnionWith(keys ...string) UnionCmd {
	return UnionCmd{tx: tx, keys: keys, aggregate: sqlx.Sum}
}

// add adds or updates the element in a set.
func (tx *Tx) add(key string, elem any, score float64) error {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return err
	}

	args := []any{key, time.Now().UnixMilli()}
	var keyID int
	err = tx.tx.QueryRow(tx.sql.add1, args...).Scan(&keyID)
	if err != nil {
		return sqlx.TypedError(err)
	}
	_, err = tx.tx.Exec(tx.sql.add2, keyID, elemb, score)
	return err
}

// count returns the number of existing elements in a set.
func (tx *Tx) count(key string, elems ...any) (int, error) {
	elembs, err := core.ToBytesMany(elems...)
	if err != nil {
		return 0, err
	}
	query, elemArgs := sqlx.ExpandIn(tx.sql.count, ":elems", elembs)
	query = tx.dialect.Enumerate(query)
	args := append([]any{key, time.Now().UnixMilli()}, elemArgs...)
	var count int
	err = tx.tx.QueryRow(query, args...).Scan(&count)
	return count, err
}

// getRank returns the rank and score of an element in a set.
func (tx *Tx) getRank(key string, elem any, sortDir string) (rank int, score float64, err error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, 0, err
	}

	args := []any{key, time.Now().UnixMilli(), elemb}
	query := tx.sql.getRank
	if sortDir != sqlx.Asc {
		query = strings.Replace(query, sqlx.Asc, sortDir, 2)
	}

	row := tx.tx.QueryRow(query, args...)
	err = row.Scan(&rank, &score)
	if err == sql.ErrNoRows {
		return 0, 0, core.ErrNotFound
	}
	if err != nil {
		return 0, 0, err
	}
	return rank, score, nil
}

// scanItem scans a set item from the current row.
func scanItem(rows *sql.Rows) (SetItem, error) {
	var it SetItem
	var elem []byte
	err := rows.Scan(&elem, &it.Score)
	if err != nil {
		return it, err
	}
	it.Elem = core.Value(elem)
	return it, nil
}

// SetItem represents an element-score pair in a sorted set.
type SetItem struct {
	id    int
	Elem  core.Value
	Score float64
}

// ScanResult is a result of the scan operation.
type ScanResult struct {
	Cursor int
	Items  []SetItem
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
