package rzset

import (
	"strings"
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
// Start and stop are 0-based, inclusive.
// Negative values are not supported.
func (c RangeCmd) ByRank(start, stop int) RangeCmd {
	c.byRank = &byRank{start, stop}
	c.byScore = nil
	return c
}

// ByScore sets filtering and sorting by score.
// Start and stop are inclusive.
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
// Only takes effect when filtering by score.
func (c RangeCmd) Offset(offset int) RangeCmd {
	c.offset = offset
	return c
}

// Count sets the maximum number of elements to return.
// Only takes effect when filtering by score.
func (c RangeCmd) Count(count int) RangeCmd {
	c.count = count
	return c
}

// Run returns a range of elements from a sorted set.
// Uses either by-rank or by-score filtering. The elements are sorted
// by score/rank and then by element according to the sorting direction.
//
// Offset and count are optional, and only take effect
// when filtering by score.
//
// If the key does not exist or is not a sorted set,
// returns a nil slice.
func (c RangeCmd) Run() ([]SetItem, error) {
	if c.byRank != nil {
		return c.rangeRank()
	}
	if c.byScore != nil {
		return c.rangeScore()
	}
	return nil, nil
}

// rangeRank retrieves a range of elements by rank.
func (c RangeCmd) rangeRank() ([]SetItem, error) {
	// Check start and stop values.
	if c.byRank.start < 0 || c.byRank.stop < 0 {
		return nil, nil
	}

	// Change sort direction if necessary.
	query := c.tx.sql.rangeRank
	if c.sortDir != sqlx.Asc {
		query = strings.Replace(query, sqlx.Asc, c.sortDir, -1)
	}

	// Prepare query arguments.
	args := []any{
		c.key,
		time.Now().UnixMilli(),
		c.byRank.start,
		c.byRank.stop,
	}

	// Execute the query.
	rows, err := c.tx.tx.Query(query, args...)
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

// rangeScore retrieves a range of elements by score.
func (c RangeCmd) rangeScore() ([]SetItem, error) {
	// Change sort direction if necessary.
	query := c.tx.sql.rangeScore
	if c.sortDir != sqlx.Asc {
		query = strings.Replace(query, sqlx.Asc, c.sortDir, -1)
	}

	// Prepare query arguments.
	args := []any{
		c.key,
		time.Now().UnixMilli(),
		c.byScore.start,
		c.byScore.stop,
	}

	// Add offset and count if necessary.
	if c.offset > 0 && c.count > 0 {
		query += " limit $5 offset $6"
		args = append(args, c.count, c.offset)
	} else if c.count > 0 {
		query += " limit $5"
		args = append(args, c.count)
	} else if c.offset > 0 {
		query += " " + c.tx.dialect.LimitAll() + " offset $5"
		args = append(args, c.offset)
	}

	// Execute the query.
	rows, err := c.tx.tx.Query(query, args...)
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
