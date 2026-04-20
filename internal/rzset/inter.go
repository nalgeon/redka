package rzset

import (
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
func (c InterCmd) Run() ([]SetItem, error) {
	if c.db != nil {
		tx := NewTx(c.db.dialect, c.db.store, c.db.ro)
		return c.run(tx)
	}
	if c.tx != nil {
		return c.run(c.tx)
	}
	return nil, nil
}

// Store intersects multiple sets and stores the result in a new set.
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

func (c InterCmd) run(tx *Tx) ([]SetItem, error) {
	rows, err := tx.z.InterRead(tx.tx, c.keys, time.Now().UnixMilli(), c.aggregate)
	if err != nil {
		return nil, err
	}
	return zElemsToSetItems(rows), nil
}

func (c InterCmd) store(tx *Tx) (int, error) {
	n, err := tx.z.InterWrite(tx.tx, c.dest, c.keys, time.Now().UnixMilli(), c.aggregate)
	if err != nil {
		return 0, tx.dialect.TypedError(err)
	}
	return n, nil
}
