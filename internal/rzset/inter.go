package rzset

import (
	"database/sql"
	"slices"
	"strings"
	"time"

	"github.com/nalgeon/redka/internal/sqlx"
)

// InterCmd intersects multiple sets.
type InterCmd struct {
	db        *DB
	tx        *Tx
	dest      string
	keys      []string
	aggregate string
}

// Dest sets the key to store the result of the intersection.
func (c InterCmd) Dest(dest string) InterCmd {
	c.dest = dest
	return c
}

// Sum changes the aggregation function to take the sum of scores.
func (c InterCmd) Sum() InterCmd {
	c.aggregate = sqlx.Sum
	return c
}

// Min changes the aggregation function to take the minimum score.
func (c InterCmd) Min() InterCmd {
	c.aggregate = sqlx.Min
	return c
}

// Max changes the aggregation function to take the maximum score.
func (c InterCmd) Max() InterCmd {
	c.aggregate = sqlx.Max
	return c
}

// Run returns the intersection of multiple sets.
// The intersection consists of elements that exist in all given sets.
// The score of each element is the aggregate of its scores in the given sets.
// If any of the source keys do not exist or are not sets, returns an empty slice.
func (c InterCmd) Run() ([]SetItem, error) {
	if c.db != nil {
		tx := NewTx(c.db.dialect, c.db.ro)
		return c.run(tx)
	}
	if c.tx != nil {
		return c.run(c.tx)
	}
	return nil, nil
}

// Store intersects multiple sets and stores the result in a new set.
// Returns the number of elements in the resulting set.
// If the destination key already exists, it is fully overwritten
// (all old elements are removed and the new ones are inserted).
// If the destination key already exists and is not a set, returns ErrKeyType.
// If any of the source keys do not exist or are not sets, does nothing,
// except deleting the destination key if it exists.
func (c InterCmd) Store() (int, error) {
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

// run returns the intersection of multiple sets.
func (c InterCmd) run(tx *Tx) ([]SetItem, error) {
	// Prepare query arguments.
	query := tx.sql.inter
	if c.aggregate != sqlx.Sum {
		query = strings.Replace(query, sqlx.Sum, c.aggregate, 2)
	}
	query, keyArgs := sqlx.ExpandIn(query, ":keys", c.keys)
	query = tx.dialect.Enumerate(query)
	args := append(
		keyArgs,                // keys
		time.Now().UnixMilli(), // now
		len(c.keys),            // nkeys
	)

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

// store intersects multiple sets and stores the result in a new set.
func (c InterCmd) store(tx *Tx) (int, error) {
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
		return 0, sqlx.TypedError(err)
	}

	// Intersect the source sets and store the result.
	query := tx.sql.interStore
	if c.aggregate != sqlx.Sum {
		query = strings.Replace(query, sqlx.Sum, c.aggregate, 2)
	}
	query, keyArgs := sqlx.ExpandIn(query, ":keys", c.keys)
	query = tx.dialect.Enumerate(query)
	args := slices.Concat([]any{destID}, keyArgs, []any{now, len(c.keys)})
	res, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	// Return the number of elements in the resulting set.
	n, _ := res.RowsAffected()
	return int(n), nil
}
