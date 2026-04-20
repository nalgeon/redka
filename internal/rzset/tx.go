package rzset

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

// Tx is a sorted set repository transaction.
type Tx struct {
	dialect sqlx.Dialect
	tx      sqlx.Tx
	z       storage.ZSetStorage
}

// NewTx creates a sorted set repository transaction
// from a generic database transaction.
func NewTx(dialect sqlx.Dialect, st storage.Storage, tx sqlx.Tx) *Tx {
	return &Tx{dialect: dialect, tx: tx, z: st.ZSets()}
}

// Add adds or updates an element in a set.
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
func (tx *Tx) AddMany(key string, items map[any]float64) (int, error) {
	elems := make([]any, 0, len(items))
	for elem := range items {
		elems = append(elems, elem)
	}
	existCount, err := tx.count(key, elems...)
	if err != nil {
		return 0, err
	}
	for elem, score := range items {
		if err := tx.add(key, elem, score); err != nil {
			return 0, err
		}
	}
	return len(items) - existCount, nil
}

// Count returns the number of elements in a set with a score between min and max.
func (tx *Tx) Count(key string, min, max float64) (int, error) {
	return tx.z.CountScore(tx.tx, key, time.Now().UnixMilli(), min, max)
}

// Delete removes elements from a set.
func (tx *Tx) Delete(key string, elems ...any) (int, error) {
	elembs, err := core.ToBytesMany(elems...)
	if err != nil {
		return 0, err
	}
	return tx.z.DeleteElems(tx.tx, key, time.Now().UnixMilli(), elembs)
}

// DeleteWith removes elements from a set with additional options.
func (tx *Tx) DeleteWith(key string) DeleteCmd {
	return DeleteCmd{tx: tx, key: key}
}

// GetRank returns the rank and score of an element in a set.
func (tx *Tx) GetRank(key string, elem any) (rank int, score float64, err error) {
	return tx.getRank(key, elem, true)
}

// GetRankRev returns the rank and score using descending order.
func (tx *Tx) GetRankRev(key string, elem any) (rank int, score float64, err error) {
	return tx.getRank(key, elem, false)
}

// GetScore returns the score of an element in a set.
func (tx *Tx) GetScore(key string, elem any) (float64, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	score, err := tx.z.GetScore(tx.tx, key, time.Now().UnixMilli(), elemb)
	if err == sql.ErrNoRows {
		return 0, core.ErrNotFound
	}
	return score, err
}

// Incr increments the score of an element in a set.
func (tx *Tx) Incr(key string, elem any, delta float64) (float64, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	keyID, err := tx.z.AddKey(tx.tx, key, time.Now().UnixMilli())
	if err != nil {
		return 0, tx.dialect.TypedError(err)
	}
	return tx.z.IncrScore(tx.tx, keyID, elemb, delta)
}

// Inter returns the intersection of multiple sets.
func (tx *Tx) Inter(keys ...string) ([]SetItem, error) {
	cmd := InterCmd{tx: tx, keys: keys, aggregate: sqlx.Sum}
	return cmd.Run()
}

// InterWith intersects multiple sets with additional options.
func (tx *Tx) InterWith(keys ...string) InterCmd {
	return InterCmd{tx: tx, keys: keys, aggregate: sqlx.Sum}
}

// Len returns the number of elements in a set.
func (tx *Tx) Len(key string) (int, error) {
	return tx.z.Len(tx.tx, key, time.Now().UnixMilli())
}

// Range returns a range of elements from a set with ranks between start and stop.
func (tx *Tx) Range(key string, start, stop int) ([]SetItem, error) {
	cmd := RangeCmd{tx: tx, key: key, sortDir: sqlx.Asc}
	return cmd.ByRank(start, stop).Run()
}

// RangeWith ranges elements from a set with additional options.
func (tx *Tx) RangeWith(key string) RangeCmd {
	return RangeCmd{tx: tx, key: key, sortDir: sqlx.Asc}
}

// Scan iterates over set items with elements matching pattern.
func (tx *Tx) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	if count == 0 {
		count = scanPageSize
	}
	rows, err := tx.z.Scan(tx.tx, key, time.Now().UnixMilli(), cursor, pattern, count)
	if err != nil {
		return ScanResult{}, err
	}
	items := make([]SetItem, 0, len(rows))
	maxID := 0
	for _, r := range rows {
		items = append(items, SetItem{id: r.RowID, Elem: core.Value(r.Elem), Score: r.Score})
		if r.RowID > maxID {
			maxID = r.RowID
		}
	}
	return ScanResult{maxID, items}, nil
}

// Scanner returns an iterator for set items with elements matching pattern.
func (tx *Tx) Scanner(key, pattern string, pageSize int) *Scanner {
	return newScanner(tx, key, pattern, pageSize)
}

// Union returns the union of multiple sets.
func (tx *Tx) Union(keys ...string) ([]SetItem, error) {
	cmd := UnionCmd{tx: tx, keys: keys, aggregate: sqlx.Sum}
	return cmd.Run()
}

// UnionWith unions multiple sets with additional options.
func (tx *Tx) UnionWith(keys ...string) UnionCmd {
	return UnionCmd{tx: tx, keys: keys, aggregate: sqlx.Sum}
}

func (tx *Tx) add(key string, elem any, score float64) error {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return err
	}
	keyID, err := tx.z.AddKey(tx.tx, key, time.Now().UnixMilli())
	if err != nil {
		return tx.dialect.TypedError(err)
	}
	return tx.z.AddScore(tx.tx, keyID, elemb, score)
}

func (tx *Tx) count(key string, elems ...any) (int, error) {
	elembs, err := core.ToBytesMany(elems...)
	if err != nil {
		return 0, err
	}
	return tx.z.CountElems(tx.tx, key, time.Now().UnixMilli(), elembs)
}

func (tx *Tx) getRank(key string, elem any, sortAsc bool) (rank int, score float64, err error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, 0, err
	}
	rank, score, err = tx.z.GetRank(tx.tx, key, time.Now().UnixMilli(), elemb, sortAsc)
	if err == sql.ErrNoRows {
		return 0, 0, core.ErrNotFound
	}
	return rank, score, err
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
