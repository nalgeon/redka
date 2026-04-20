package rzset

import (
	"time"
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
func (c DeleteCmd) ByRank(start, stop int) DeleteCmd {
	c.byRank = &byRank{start, stop}
	c.byScore = nil
	return c
}

// ByScore sets filtering by score.
func (c DeleteCmd) ByScore(start, stop float64) DeleteCmd {
	c.byScore = &byScore{start, stop}
	c.byRank = nil
	return c
}

// Run removes elements from a set according to the specified criteria.
func (c DeleteCmd) Run() (int, error) {
	if c.db != nil {
		var n int
		err := c.db.update(func(tx *Tx) error {
			var err error
			n, err = c.run(tx)
			return err
		})
		return n, err
	}
	if c.tx != nil {
		return c.run(c.tx)
	}
	return 0, nil
}

func (c DeleteCmd) run(tx *Tx) (n int, err error) {
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

	err = tx.z.UpdateKeyLen(tx.tx, now, n, c.key, now)
	return n, err
}

func (c DeleteCmd) deleteRank(tx *Tx, now int64) (int, error) {
	if c.byRank.start < 0 || c.byRank.stop < 0 {
		return 0, nil
	}
	limit := c.byRank.stop - c.byRank.start + 1
	return tx.z.DeleteByRank(tx.tx, c.key, now, limit, c.byRank.start)
}

func (c DeleteCmd) deleteScore(tx *Tx, now int64) (int, error) {
	return tx.z.DeleteByScore(tx.tx, c.key, now, c.byScore.start, c.byScore.stop)
}
