package rzset

import (
	"database/sql"
	"slices"
	"strings"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

const (
	sqlInter = `
	select elem, sum(score) as score
	from rzset
	  join rkey on key_id = rkey.id and (etime is null or etime > :now)
	where key in (:keys)
	group by elem
	having count(distinct key_id) = :nkeys
	order by sum(score), elem`

	sqlInterStore = `
	delete from rzset
	where key_id = (
		select id from rkey where key = :key
		and (etime is null or etime > :now)
	  );

	insert into rkey (key, type, version, mtime)
	values (:key, :type, :version, :mtime)
	on conflict (key) do update set
	  version = version+1,
	  type = excluded.type,
	  mtime = excluded.mtime;

	insert into rzset (key_id, elem, score)
	select
	  (select id from rkey where key = :key),
	  elem, sum(score) as score
	from rzset
	  join rkey on key_id = rkey.id and (etime is null or etime > :now)
	where key in (:keys)
	group by elem
	having count(distinct key_id) = :nkeys
	order by sum(score), elem;`
)

// InterCmd intersects multiple sets.
type InterCmd struct {
	tx        sqlx.Tx
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
	// Prepare query arguments.
	query := sqlInter
	if c.aggregate != sqlx.Sum {
		query = strings.Replace(query, sqlx.Sum, c.aggregate, 2)
	}
	query, keyArgs := sqlx.ExpandIn(query, ":keys", c.keys)
	args := slices.Concat(
		[]any{time.Now().UnixMilli()}, // now
		keyArgs,                       // keys
		[]any{len(c.keys)},            // nkeys
	)

	// Execute the query.
	var rows *sql.Rows
	rows, err := c.tx.Query(query, args...)
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

// Store intersects multiple sets and stores the result in a new set.
// Returns the number of elements in the resulting set.
// If the destination key already exists, it is fully overwritten
// (all old elements are removed and the new ones are inserted).
// If any of the source keys do not exist or are not sets, does nothing,
// except deleting the destination key if it exists.
func (c InterCmd) Store() (int, error) {
	// Insert the destination key and get its ID.
	now := time.Now().UnixMilli()
	args := []any{
		// delete from rzset
		c.dest, // key
		now,    // now
		// insert into rkey
		c.dest,              // key
		core.TypeSortedSet,  // type
		core.InitialVersion, // version
		now,                 // mtime
		// insert into rzset
		c.dest, // key
		now,    // now
		// keys
		// nkeys
	}
	query := sqlInterStore
	if c.aggregate != sqlx.Sum {
		query = strings.Replace(query, sqlx.Sum, c.aggregate, 2)
	}
	query, keyArgs := sqlx.ExpandIn(query, ":keys", c.keys)
	args = slices.Concat(args, keyArgs, []any{len(c.keys)})

	res, err := c.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	// Return the number of elements in the resulting set.
	n, _ := res.RowsAffected()
	return int(n), nil
}
