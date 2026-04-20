package rzset

import (
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
	"github.com/nalgeon/redka/internal/storage"
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
func (c UnionCmd) Run() ([]SetItem, error) {
	if c.db != nil {
		tx := NewTx(c.db.dialect, c.db.store, c.db.ro)
		return c.run(tx)
	}
	if c.tx != nil {
		return c.run(c.tx)
	}
	return nil, nil
}

// Store unions multiple sets and stores the result in a new set.
func (c UnionCmd) Store() (int, error) {
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

func (c UnionCmd) run(tx *Tx) ([]SetItem, error) {
	rows, err := tx.z.UnionRead(tx.tx, c.keys, time.Now().UnixMilli(), c.aggregate)
	if err != nil {
		return nil, err
	}
	return zElemsToSetItems(rows), nil
}

func (c UnionCmd) store(tx *Tx) (int, error) {
	n, err := tx.z.UnionWrite(tx.tx, c.dest, c.keys, time.Now().UnixMilli(), c.aggregate)
	if err != nil {
		return 0, tx.dialect.TypedError(err)
	}
	return n, nil
}

func zElemsToSetItems(zs []storage.ZSetElem) []SetItem {
	if len(zs) == 0 {
		return nil
	}
	out := make([]SetItem, len(zs))
	for i, z := range zs {
		out[i] = SetItem{Elem: core.Value(z.Elem), Score: z.Score}
	}
	return out
}
