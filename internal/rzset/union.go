package rzset

import (
	"database/sql"
	"strings"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

const (
	sqlUnion = `
	select elem, sum(score) as score
	from rzset
	  join rkey on key_id = rkey.id and (etime is null or etime > ?)
	where key in (:keys)
	group by elem
	order by sum(score), elem`

	sqlUnionStore1 = `
	delete from rzset
	where key_id = (
		select id from rkey where key = ?
		and (etime is null or etime > ?)
	  )`

	sqlUnionStore2 = `
	insert into rkey (key, type, version, mtime)
	values (?, ?, ?, ?)
	on conflict (key) do update set
	  version = version+1,
	  type = excluded.type,
	  mtime = excluded.mtime
	returning id`

	sqlUnionStore3 = `
	insert into rzset (key_id, elem, score)
	select ?, elem, sum(score) as score
	from rzset
	  join rkey on key_id = rkey.id and (etime is null or etime > ?)
	where key in (:keys)
	group by elem
	order by sum(score), elem;`
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
	args := append([]any{now}, keyArgs...)

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
	args := []any{c.dest, now}
	_, err := tx.Exec(sqlUnionStore1, args...)
	if err != nil {
		return 0, err
	}

	// Create the destination key.
	args = []any{
		c.dest,              // key
		core.TypeSortedSet,  // type
		core.InitialVersion, // version
		now,                 // mtime
	}
	var destID int
	err = tx.QueryRow(sqlUnionStore2, args...).Scan(&destID)
	if err != nil {
		return 0, err
	}

	// Union the source sets and store the result.
	query := sqlUnionStore3
	if c.aggregate != sqlx.Sum {
		query = strings.Replace(query, sqlx.Sum, c.aggregate, 2)
	}
	query, keyArgs := sqlx.ExpandIn(query, ":keys", c.keys)
	args = append([]any{destID, now}, keyArgs...)
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	// Return the number of elements in the resulting set.
	n, _ := res.RowsAffected()
	return int(n), nil
}
