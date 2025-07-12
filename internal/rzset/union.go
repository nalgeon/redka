package rzset

import (
	"database/sql"
	"slices"
	"strings"
	"time"

	"github.com/nalgeon/redka/internal/sqlx"
)

// UnionCmd unions multiple sets.
type UnionCmd struct {
	db        *DB
	tx        *Tx
	dest      string
	keys      []string
	aggregate string
}

// Dest sets the key to store the result of the union.
func (c UnionCmd) Dest(dest string) UnionCmd {
	c.dest = dest
	return c
}

// Sum changes the aggregation function to take the sum of scores.
func (c UnionCmd) Sum() UnionCmd {
	c.aggregate = sqlx.Sum
	return c
}

// Min changes the aggregation function to take the minimum score.
func (c UnionCmd) Min() UnionCmd {
	c.aggregate = sqlx.Min
	return c
}

// Max changes the aggregation function to take the maximum score.
func (c UnionCmd) Max() UnionCmd {
	c.aggregate = sqlx.Max
	return c
}

// Run returns the union of multiple sets.
// The union consists of elements that exist in any of the given sets.
// The score of each element is the aggregate of its scores in the given sets.
// Ignores the keys that do not exist or are not sets.
// If no keys exist, returns a nil slice.
func (c UnionCmd) Run() ([]SetItem, error) {
	if c.db != nil {
		tx := NewTx(c.db.dialect, c.db.ro)
		return c.run(tx)
	}
	if c.tx != nil {
		return c.run(c.tx)
	}
	return nil, nil
}

// Store unions multiple sets and stores the result in a new set.
// Returns the number of elements in the resulting set.
// If the destination key already exists, it is fully overwritten
// (all old elements are removed and the new ones are inserted).
// If the destination key already exists and is not a set, returns ErrKeyType.
// Ignores the source keys that do not exist or are not sets.
// If all of the source keys do not exist or are not sets, does nothing,
// except deleting the destination key if it exists.
func (c UnionCmd) Store() (int, error) {
	if c.db != nil {
		var count int
		err := c.db.update(func(tx *Tx) error {
			var err error
			count, err = c.store(tx)
			return err
		})
		return count, err
	}
	if c.tx != nil {
		return c.store(c.tx)
	}
	return 0, nil
}

// run returns the union of multiple sets.
func (c UnionCmd) run(tx *Tx) ([]SetItem, error) {
	// Prepare query arguments.
	now := time.Now().UnixMilli()
	query := tx.sql.union
	if c.aggregate != sqlx.Sum {
		query = strings.Replace(query, sqlx.Sum, c.aggregate, 2)
	}
	query, keyArgs := sqlx.ExpandIn(query, ":keys", c.keys)
	query = tx.dialect.Enumerate(query)
	args := append(keyArgs, now)

	// Execute the query.
	var rows *sql.Rows
	rows, err := tx.tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	// Build the resulting element-score slice.
	var items []SetItem
	for rows.Next() {
		it, err := scanItem(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return items, nil
}

// store unions multiple sets and stores the result in a new set.
func (c UnionCmd) store(tx *Tx) (int, error) {
	now := time.Now().UnixMilli()

	// Delete the destination key if it exists.
	_, err := tx.tx.Exec(tx.sql.deleteAll1, c.dest, now)
	if err != nil {
		return 0, err
	}
	_, err = tx.tx.Exec(tx.sql.deleteAll2, c.dest, now)
	if err != nil {
		return 0, err
	}

	// Create the destination key.
	var destID int
	err = tx.tx.QueryRow(tx.sql.add1, c.dest, now).Scan(&destID)
	if err != nil {
		return 0, tx.dialect.TypedError(err)
	}

	// Union the source sets and store the result.
	query := tx.sql.unionStore
	if c.aggregate != sqlx.Sum {
		query = strings.Replace(query, sqlx.Sum, c.aggregate, 2)
	}
	query, keyArgs := sqlx.ExpandIn(query, ":keys", c.keys)
	query = tx.dialect.Enumerate(query)
	args := slices.Concat([]any{destID}, keyArgs, []any{now})
	res, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	// Return the number of elements in the resulting set.
	n, _ := res.RowsAffected()
	return int(n), nil
}
