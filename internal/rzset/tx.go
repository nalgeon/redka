package rzset

import (
	"database/sql"
	"strings"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

const (
	sqlAdd1 = `
	insert into rkey (key, type, version, mtime)
	values (?, ?, ?, ?)
	on conflict (key) do update set
		version = version+1,
		type = excluded.type,
		mtime = excluded.mtime`

	sqlAdd2 = `
	insert into rzset (key_id, elem, score)
	values ((select id from rkey where key = ?), ?, ?)
	on conflict (key_id, elem) do update
	set score = excluded.score`

	sqlCount = `
	select count(elem)
	from rzset
		join rkey on key_id = rkey.id and (etime is null or etime > ?)
	where key = ? and elem in (:elems)`

	sqlCountScore = `
	select count(elem)
	from rzset
		join rkey on key_id = rkey.id and (etime is null or etime > ?)
	where key = ? and score between ? and ?`

	sqlDelete = `
	delete from rzset
	where key_id = (
			select id from rkey where key = ?
			and (etime is null or etime > ?)
		) and elem in (:elems)`

	sqlGetRank = `
	with ranked as (
		select elem, score, (row_number() over w - 1) as rank
		from rzset
			join rkey on key_id = rkey.id and (etime is null or etime > ?)
		where key = ?
		window w as (partition by key_id order by score asc, elem asc)
	)
	select rank, score
	from ranked
	where elem = ?`

	sqlGetScore = `
	select score
	from rzset
		join rkey on key_id = rkey.id and (etime is null or etime > ?)
	where key = ? and elem = ?`

	sqlIncr1 = `
	insert into rkey (key, type, version, mtime)
	values (?, ?, ?, ?)
	on conflict (key) do update set
		version = version+1,
		type = excluded.type,
		mtime = excluded.mtime`

	sqlIncr2 = `
	insert into rzset (key_id, elem, score)
	values ((select id from rkey where key = ?), ?, ?)
	on conflict (key_id, elem) do update
	set score = score + excluded.score
	returning score`

	sqlLen = `
	select count(elem)
	from rzset
		join rkey on key_id = rkey.id and (etime is null or etime > ?)
	where key = ?`

	sqlScan = `
	select rzset.rowid, elem, score
	from rzset
		join rkey on key_id = rkey.id and (etime is null or etime > ?)
	where key = ? and rzset.rowid > ? and elem glob ?
	limit ?`
)

const scanPageSize = 10

// Tx is a sorted set repository transaction.
type Tx struct {
	tx sqlx.Tx
}

// NewTx creates a sorted set repository transaction
// from a generic database transaction.
func NewTx(tx sqlx.Tx) *Tx {
	return &Tx{tx}
}

// Add adds or updates an element in a set.
// Returns true if the element was created, false if it was updated.
// If the key does not exist, creates it.
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
		time.Now().UnixMilli(),
		key, min, max,
	}
	var n int
	err := tx.tx.QueryRow(sqlCountScore, args...).Scan(&n)
	return n, err
}

// Delete removes elements from a set.
// Returns the number of elements removed.
// Ignores the elements that do not exist.
// Does nothing if the key does not exist or is not a set.
// Does not delete the key if the set becomes empty.
func (tx *Tx) Delete(key string, elems ...any) (int, error) {
	// Check the types of the elements.
	elembs, err := core.ToBytesMany(elems...)
	if err != nil {
		return 0, err
	}

	// Remove the elements.
	query, elemArgs := sqlx.ExpandIn(sqlDelete, ":elems", elembs)
	args := append([]any{key, time.Now().UnixMilli()}, elemArgs...)
	res, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	count, _ := res.RowsAffected()
	return int(count), nil
}

// DeleteWith removes elements from a set with additional options.
func (tx *Tx) DeleteWith(key string) DeleteCmd {
	return DeleteCmd{tx: tx.tx, key: key}
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
	args := []any{time.Now().UnixMilli(), key, elemb}
	row := tx.tx.QueryRow(sqlGetScore, args...)
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
func (tx *Tx) Incr(key string, elem any, delta float64) (float64, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}

	args := []any{
		key,                    // key
		core.TypeSortedSet,     // type
		core.InitialVersion,    // version
		time.Now().UnixMilli(), // mtime
	}
	_, err = tx.tx.Exec(sqlIncr1, args...)
	if err != nil {
		return 0, err
	}

	var score float64
	args = []any{key, elemb, delta}
	err = tx.tx.QueryRow(sqlIncr2, args...).Scan(&score)
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
	args := []any{time.Now().UnixMilli(), key}
	err := tx.tx.QueryRow(sqlLen, args...).Scan(&n)
	return n, err
}

// Range returns a range of elements from a set with ranks between start and stop.
// The rank is the 0-based position of the element in the set, ordered
// by score (from low to high), and then by lexicographical order (ascending).
// Start and stop are 0-based, inclusive. Negative values are not supported.
// If the key does not exist or is not a set, returns a nil slice.
func (tx *Tx) Range(key string, start, stop int) ([]SetItem, error) {
	cmd := RangeCmd{tx: tx.tx, key: key, sortDir: sqlx.Asc}
	return cmd.ByRank(start, stop).Run()
}

// RangeWith ranges elements from a set with additional options.
func (tx *Tx) RangeWith(key string) RangeCmd {
	return RangeCmd{tx: tx.tx, key: key, sortDir: sqlx.Asc}
}

// Scan iterates over set items with elements matching pattern.
// Returns a slice of element-score pairs (see [SetItem]) of size count
// based on the current state of the cursor. Returns an empty SetItem
// slice when there are no more items.
// If the key does not exist or is not a set, returns a nil slice.
// Supports glob-style patterns. Set count = 0 for default page size.
func (tx *Tx) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	if count == 0 {
		count = scanPageSize
	}

	// Select set items matching the pattern.
	args := []any{
		time.Now().UnixMilli(), key,
		cursor, pattern, count,
	}
	scan := func(rows *sql.Rows) (SetItem, error) {
		var it SetItem
		var elem []byte
		err := rows.Scan(&it.id, &elem, &it.Score)
		it.Elem = core.Value(elem)
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

	args := []any{
		key,                    // key
		core.TypeSortedSet,     // type
		core.InitialVersion,    // version
		time.Now().UnixMilli(), // mtime
	}
	_, err = tx.tx.Exec(sqlAdd1, args...)
	if err != nil {
		return err
	}
	_, err = tx.tx.Exec(sqlAdd2, key, elemb, score)
	return err
}

// count returns the number of existing elements in a set.
func (tx *Tx) count(key string, elems ...any) (int, error) {
	elembs, err := core.ToBytesMany(elems...)
	if err != nil {
		return 0, err
	}
	query, elemArgs := sqlx.ExpandIn(sqlCount, ":elems", elembs)
	args := append([]any{time.Now().UnixMilli(), key}, elemArgs...)
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

	args := []any{time.Now().UnixMilli(), key, elemb}
	query := sqlGetRank
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
