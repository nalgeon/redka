package rzset

import (
	"time"

	"github.com/nalgeon/redka/internal/sqlx"
)

const (
	sqlDeleteRank = `
	with ranked as (
		select rowid, elem, score
		from rzset
		where key_id = (
			select id from rkey
			where key = ? and type = 5 and (etime is null or etime > ?)
		)
		order by score, elem
		limit ?, ?
	)
	delete from rzset
	where rowid in (select rowid from ranked)`

	sqlDeleteScore = `
	delete from rzset
	where key_id = (
			select id from rkey
			where key = ? and type = 5 and (etime is null or etime > ?)
		) and score between ? and ?`

	sqlUpdateKey = sqlDelete2
)

// DeleteCmd removes elements from a set.
type DeleteCmd struct {
	db      *DB
	tx      *Tx
	key     string
	byRank  *byRank
	byScore *byScore
}

// ByRank sets filtering by rank.
// The rank is the 0-based position of the element in the set, ordered
// by score (from high to low), and then by lexicographical order (descending).
// Start and stop are 0-based, inclusive. Negative values are not supported.
func (c DeleteCmd) ByRank(start, stop int) DeleteCmd {
	c.byRank = &byRank{start, stop}
	c.byScore = nil
	return c
}

// ByScore sets filtering by score.
// Start and stop are inclusive.
func (c DeleteCmd) ByScore(start, stop float64) DeleteCmd {
	c.byScore = &byScore{start, stop}
	c.byRank = nil
	return c
}

// Run removes elements from a set according to the
// specified criteria (rank or score range).
// Returns the number of elements removed.
// Does nothing if the key does not exist or is not a set.
func (c DeleteCmd) Run() (int, error) {
	if c.db != nil {
		var n int
		err := c.db.Update(func(tx *Tx) error {
			var err error
			n, err = c.run(tx.tx)
			return err
		})
		return n, err
	}
	if c.tx != nil {
		return c.run(c.tx.tx)
	}
	return 0, nil
}

func (c DeleteCmd) run(tx sqlx.Tx) (n int, err error) {
	now := time.Now().UnixMilli()

	if c.byRank != nil {
		n, err = c.deleteRank(tx, now)
	} else if c.byScore != nil {
		n, err = c.deleteScore(tx, now)
	} else {
		return 0, nil
	}
	if err != nil || n == 0 {
		return 0, err
	}

	err = c.updateKey(tx, now, n)
	return n, err
}

// deleteRank removes elements from a set by rank.
// Returns the number of elements removed.
func (c DeleteCmd) deleteRank(tx sqlx.Tx, now int64) (int, error) {
	// Check start and stop values.
	if c.byRank.start < 0 || c.byRank.stop < 0 {
		return 0, nil
	}

	// Delete elements by rank.
	args := []any{
		c.key,                              // key
		now,                                // now
		c.byRank.start,                     // start
		c.byRank.stop - c.byRank.start + 1, // count
	}
	res, err := tx.Exec(sqlDeleteRank, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// deleteScore removes elements from a set by score.
// Returns the number of elements removed.
func (c DeleteCmd) deleteScore(tx sqlx.Tx, now int64) (int, error) {
	args := []any{
		c.key,
		now,
		c.byScore.start,
		c.byScore.stop,
	}
	res, err := tx.Exec(sqlDeleteScore, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// updateKey updates the key after deleting the elements.
func (c DeleteCmd) updateKey(tx sqlx.Tx, now int64, n int) error {
	args := []any{now, n, c.key, now}
	_, err := tx.Exec(sqlUpdateKey, args...)
	return err
}
