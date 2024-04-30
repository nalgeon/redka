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
			where key = ? and (etime is null or etime > ?)
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
			where key = ? and (etime is null or etime > ?)
		) and score between ? and ?`
)

// DeleteCmd removes elements from a set.
type DeleteCmd struct {
	tx      sqlx.Tx
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
// Does not delete the key if the set becomes empty.
func (c DeleteCmd) Run() (int, error) {
	if c.byRank != nil {
		return c.deleteRank()
	}
	if c.byScore != nil {
		return c.deleteScore()
	}
	return 0, nil
}

// deleteRank removes elements from a set by rank.
func (c DeleteCmd) deleteRank() (int, error) {
	// Check start and stop values.
	if c.byRank.start < 0 || c.byRank.stop < 0 {
		return 0, nil
	}

	// Delete elements by rank.
	args := []any{
		c.key,                              // key
		time.Now().UnixMilli(),             // now
		c.byRank.start,                     // start
		c.byRank.stop - c.byRank.start + 1, // count
	}
	res, err := c.tx.Exec(sqlDeleteRank, args...)
	if err != nil {
		return 0, err
	}
	count, _ := res.RowsAffected()
	return int(count), nil
}

// deleteScore removes elements from a set by score.
func (c DeleteCmd) deleteScore() (int, error) {
	args := []any{
		c.key,
		time.Now().UnixMilli(),
		c.byScore.start,
		c.byScore.stop,
	}
	res, err := c.tx.Exec(sqlDeleteScore, args...)
	if err != nil {
		return 0, err
	}
	count, _ := res.RowsAffected()
	return int(count), nil
}
