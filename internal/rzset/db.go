// Package rzset is a database-backed sorted set repository.
// It provides methods to interact with sorted sets in the database.
package rzset

import (
	"database/sql"

	"github.com/nalgeon/redka/internal/sqlx"
)

// DB is a database-backed sorted set repository.
// A sorted set (zset) is a like a set, but each element has a score.
// While elements are unique, scores can be repeated.
//
// Elements in the set are ordered by score (from low to high), and then
// by lexicographical order (ascending). Adding, updating or removing
// elements maintains the order of the set.
//
// Use the sorted set repository to work with sets and their elements,
// and to perform set operations like union or intersection.
type DB struct {
	*sqlx.DB[*Tx]
}

// New connects to the sorted set repository.
// Does not create the database schema.
func New(db *sql.DB) *DB {
	d := sqlx.New(db, NewTx)
	return &DB{d}
}

// Add adds or updates an element in a set.
// Returns true if the element was created, false if it was updated.
// If the key does not exist, creates it.
func (d *DB) Add(key string, elem any, score float64) (bool, error) {
	var created bool
	err := d.Update(func(tx *Tx) error {
		var err error
		created, err = tx.Add(key, elem, score)
		return err
	})
	return created, err

}

// AddMany adds or updates multiple elements in a set.
// Returns the number of elements created (as opposed to updated).
// If the key does not exist, creates it.
func (d *DB) AddMany(key string, items map[any]float64) (int, error) {
	var count int
	err := d.Update(func(tx *Tx) error {
		var err error
		count, err = tx.AddMany(key, items)
		return err
	})
	return count, err
}

// Count returns the number of elements in a set with a score between
// min and max (inclusive). Exclusive ranges are not supported.
// Returns 0 if the key does not exist or is not a set.
func (d *DB) Count(key string, min, max float64) (int, error) {
	tx := NewTx(d.SQL)
	return tx.Count(key, min, max)
}

// Delete removes elements from a set.
// Returns the number of elements removed.
// Ignores the elements that do not exist.
// Does nothing if the key does not exist or is not a set.
// Does not delete the key if the set becomes empty.
func (d *DB) Delete(key string, elems ...any) (int, error) {
	var count int
	err := d.Update(func(tx *Tx) error {
		var err error
		count, err = tx.Delete(key, elems...)
		return err
	})
	return count, err
}

// DeleteWith removes elements from a set with additional options.
func (d *DB) DeleteWith(key string) DeleteCmd {
	return DeleteCmd{db: d, key: key}
}

// GetRank returns the rank and score of an element in a set.
// The rank is the 0-based position of the element in the set, ordered
// by score (from low to high), and then by lexicographical order (ascending).
// If the element does not exist, returns ErrNotFound.
// If the key does not exist or is not a set, returns ErrNotFound.
func (d *DB) GetRank(key string, elem any) (rank int, score float64, err error) {
	tx := NewTx(d.SQL)
	return tx.GetRank(key, elem)
}

// GetRankRev returns the rank and score of an element in a set.
// The rank is the 0-based position of the element in the set, ordered
// by score (from high to low), and then by lexicographical order (descending).
// If the element does not exist, returns ErrNotFound.
// If the key does not exist or is not a set, returns ErrNotFound.
func (d *DB) GetRankRev(key string, elem any) (rank int, score float64, err error) {
	tx := NewTx(d.SQL)
	return tx.GetRankRev(key, elem)
}

// GetScore returns the score of an element in a set.
// If the element does not exist, returns ErrNotFound.
// If the key does not exist or is not a set, returns ErrNotFound.
func (d *DB) GetScore(key string, elem any) (float64, error) {
	tx := NewTx(d.SQL)
	return tx.GetScore(key, elem)
}

// Incr increments the score of an element in a set.
// Returns the score after the increment.
// If the element does not exist, adds it and sets the score to 0.0
// before the increment. If the key does not exist, creates it.
func (d *DB) Incr(key string, elem any, delta float64) (float64, error) {
	var score float64
	err := d.Update(func(tx *Tx) error {
		var err error
		score, err = tx.Incr(key, elem, delta)
		return err
	})
	return score, err
}

// Inter returns the intersection of multiple sets.
// The intersection consists of elements that exist in all given sets.
// The score of each element is the sum of its scores in the given sets.
// If any of the source keys do not exist or are not sets, returns an empty slice.
func (d *DB) Inter(keys ...string) ([]SetItem, error) {
	tx := NewTx(d.SQL)
	return tx.Inter(keys...)
}

// InterWith intersects multiple sets with additional options.
func (d *DB) InterWith(keys ...string) InterCmd {
	return InterCmd{db: d, keys: keys, aggregate: sqlx.Sum}
}

// Len returns the number of elements in a set.
// Returns 0 if the key does not exist or is not a set.
func (d *DB) Len(key string) (int, error) {
	tx := NewTx(d.SQL)
	return tx.Len(key)
}

// Range returns a range of elements from a set with ranks between start and stop.
// The rank is the 0-based position of the element in the set, ordered
// by score (from low to high), and then by lexicographical order (ascending).
// Start and stop are 0-based, inclusive. Negative values are not supported.
// If the key does not exist or is not a set, returns a nil slice.
func (d *DB) Range(key string, start, stop int) ([]SetItem, error) {
	tx := NewTx(d.SQL)
	return tx.Range(key, start, stop)
}

// RangeWith ranges elements from a set with additional options.
func (d *DB) RangeWith(key string) RangeCmd {
	tx := NewTx(d.SQL)
	return tx.RangeWith(key)
}

// Scan iterates over set items with elements matching pattern.
// Returns a slice of element-score pairs (see [SetItem]) of size count
// based on the current state of the cursor. Returns an empty SetItem
// slice when there are no more items.
// If the key does not exist or is not a set, returns a nil slice.
// Supports glob-style patterns. Set count = 0 for default page size.
func (d *DB) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	tx := NewTx(d.SQL)
	return tx.Scan(key, cursor, pattern, count)
}

// Scanner returns an iterator for set items with elements matching pattern.
// The scanner returns items one by one, fetching them from the database
// in pageSize batches when necessary. Stops when there are no more items
// or an error occurs. If the key does not exist or is not a set, stops immediately.
// Supports glob-style patterns. Set pageSize = 0 for default page size.
func (d *DB) Scanner(key, pattern string, pageSize int) *Scanner {
	tx := NewTx(d.SQL)
	return tx.Scanner(key, pattern, pageSize)
}

// Union returns the union of multiple sets.
// The union consists of elements that exist in any of the given sets.
// The score of each element is the sum of its scores in the given sets.
// Ignores the keys that do not exist or are not sets.
// If no keys exist, returns a nil slice.
func (d *DB) Union(keys ...string) ([]SetItem, error) {
	tx := NewTx(d.SQL)
	return tx.Union(keys...)
}

// UnionWith unions multiple sets with additional options.
func (d *DB) UnionWith(keys ...string) UnionCmd {
	return UnionCmd{db: d, keys: keys, aggregate: sqlx.Sum}
}
