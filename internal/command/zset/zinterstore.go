package zset

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/sqlx"
)

// Stores the intersect of multiple sorted sets in a key.
// ZINTERSTORE dest numkeys key [key ...] [AGGREGATE <SUM | MIN | MAX>]
// https://redis.io/commands/zinterstore
type ZInterStore struct {
	redis.BaseCmd
	dest      string
	keys      []string
	aggregate string
}

func ParseZInterStore(b redis.BaseCmd) (ZInterStore, error) {
	cmd := ZInterStore{BaseCmd: b}
	var nKeys int
	err := parser.New(
		parser.String(&cmd.dest),
		parser.Int(&nKeys),
		parser.StringsN(&cmd.keys, &nKeys),
		parser.Named("aggregate", parser.Enum(&cmd.aggregate, sqlx.Sum, sqlx.Min, sqlx.Max)),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return ZInterStore{}, err
	}
	return cmd, nil
}

func (cmd ZInterStore) Run(w redis.Writer, red redis.Redka) (any, error) {
	inter := red.ZSet().InterWith(cmd.keys...).Dest(cmd.dest)
	switch cmd.aggregate {
	case sqlx.Min:
		inter = inter.Min()
	case sqlx.Max:
		inter = inter.Max()
	case sqlx.Sum:
		inter = inter.Sum()
	}

	count, err := inter.Store()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	w.WriteInt(count)
	return count, nil
}
