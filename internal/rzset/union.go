package rzset

import (
	"database/sql"
	"slices"
	"strings"
	"time"

	"github.com/nalgeon/redka/internal/sqlx"
)

const (
	sqlUnion = `
	select elem, sum(score) as score
	from rzset join rkey on kid = rkey.id and type = 5
	where key in (:keys) and (etime is null or etime > ?)
	group by elem
	order by sum(score), elem`

	sqlUnionStore1 = sqlDeleteAll1
	sqlUnionStore2 = sqlDeleteAll2
	sqlUnionStore3 = sqlAdd1
	sqlUnionStore4 = `
	insert into rzset (kid, elem, score)
	select ?, elem, sum(score) as score
	from rzset join rkey on kid = rkey.id and type = 5
	where key in (:keys) and (etime is null or etime > ?)
	group by elem
	order by sum(score), elem`
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
		return c.run(c.db.RO)
	}
	if c.tx != nil {
		return c.run(c.tx.tx)
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
		err := c.db.Update(func(tx *Tx) error {
			var err error
			count, err = c.store(tx.tx)
			return err
		})
		return count, err
	}
	if c.tx != nil {
		return c.store(c.tx.tx)
	}
	return 0, nil
}

// run returns the union of multiple sets.
func (c UnionCmd) run(tx sqlx.Tx) ([]SetItem, error) {
	// Prepare query arguments.
	now := time.Now().UnixMilli()
	query := sqlUnion
	if c.aggregate != sqlx.Sum {
		query = strings.Replace(query, sqlx.Sum, c.aggregate, 2)
	}
	query, keyArgs := sqlx.ExpandIn(query, ":keys", c.keys)
	args := append(keyArgs, now)

	// Execute the query.
	var rows *sql.Rows
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
func (c UnionCmd) store(tx sqlx.Tx) (int, error) {
	now := time.Now().UnixMilli()

	// Delete the destination key if it exists.
	_, err := tx.Exec(sqlUnionStore1, c.dest, now)
	if err != nil {
		return 0, err
	}
	_, err = tx.Exec(sqlUnionStore2, c.dest, now)
	if err != nil {
		return 0, err
	}

	// Create the destination key.
	var destID int
	err = tx.QueryRow(sqlUnionStore3, c.dest, now).Scan(&destID)
	if err != nil {
		return 0, sqlx.TypedError(err)
	}

	// Union the source sets and store the result.
	query := sqlUnionStore4
	if c.aggregate != sqlx.Sum {
		query = strings.Replace(query, sqlx.Sum, c.aggregate, 2)
	}
	query, keyArgs := sqlx.ExpandIn(query, ":keys", c.keys)
	args := slices.Concat([]any{destID}, keyArgs, []any{now})
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	// Return the number of elements in the resulting set.
	n, _ := res.RowsAffected()
	return int(n), nil
}
