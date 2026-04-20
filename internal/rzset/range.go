package rzset

import (
	"time"

	"github.com/nalgeon/redka/internal/sqlx"
)

type byRank struct {
	start, stop int
}

type byScore struct {
	start, stop float64
}

// RangeCmd retrieves a range of elements from a sorted set.
type RangeCmd struct {
	tx      *Tx
	key     string
	byRank  *byRank
	byScore *byScore
	sortDir string
	offset  int
	count   int
}

// ByRank sets filtering and sorting by rank.
func (c RangeCmd) ByRank(start, stop int) RangeCmd {
	c.byRank = &byRank{start, stop}
	c.byScore = nil
	return c
}

// ByScore sets filtering and sorting by score.
func (c RangeCmd) ByScore(start, stop float64) RangeCmd {
	c.byScore = &byScore{start, stop}
	c.byRank = nil
	return c
}

// Asc sets the sorting direction to ascending.
func (c RangeCmd) Asc() RangeCmd {
	c.sortDir = sqlx.Asc
	return c
}

// Desc sets the sorting direction to descending.
func (c RangeCmd) Desc() RangeCmd {
	c.sortDir = sqlx.Desc
	return c
}

// Offset sets the offset of the range.
func (c RangeCmd) Offset(offset int) RangeCmd {
	c.offset = offset
	return c
}

// Count sets the maximum number of elements to return.
func (c RangeCmd) Count(count int) RangeCmd {
	c.count = count
	return c
}

// Run returns a range of elements from a sorted set.
func (c RangeCmd) Run() ([]SetItem, error) {
	if c.byRank != nil {
		return c.rangeRank()
	}
	if c.byScore != nil {
		return c.rangeScore()
	}
	return nil, nil
}

func (c RangeCmd) rangeRank() ([]SetItem, error) {
	if c.byRank.start < 0 || c.byRank.stop < 0 {
		return nil, nil
	}
	sortAsc := c.sortDir == sqlx.Asc
	rows, err := c.tx.z.RangeRank(c.tx.tx, c.key, time.Now().UnixMilli(),
		c.byRank.start, c.byRank.stop, sortAsc)
	if err != nil {
		return nil, err
	}
	return zElemsToSetItems(rows), nil
}

func (c RangeCmd) rangeScore() ([]SetItem, error) {
	sortAsc := c.sortDir == sqlx.Asc
	rows, err := c.tx.z.RangeScore(c.tx.tx, c.key, time.Now().UnixMilli(),
		c.byScore.start, c.byScore.stop, sortAsc, c.offset, c.count)
	if err != nil {
		return nil, err
	}
	return zElemsToSetItems(rows), nil
}
