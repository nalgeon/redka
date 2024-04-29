package zset

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/sqlx"
)

// Stores the union of multiple sorted sets in a key.
// ZUNIONSTORE dest numkeys key [key ...] [AGGREGATE <SUM | MIN | MAX>]
// https://redis.io/commands/zunionstore
type ZUnionStore struct {
	redis.BaseCmd
	Dest      string
	Keys      []string
	Aggregate string
}

func ParseZUnionStore(b redis.BaseCmd) (*ZUnionStore, error) {
	cmd := &ZUnionStore{BaseCmd: b}
	var nKeys int
	err := parser.New(
		parser.String(&cmd.Dest),
		parser.Int(&nKeys),
		parser.StringsN(&cmd.Keys, &nKeys),
		parser.Named("aggregate", parser.Enum(&cmd.Aggregate, sqlx.Sum, sqlx.Min, sqlx.Max)),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZUnionStore) Run(w redis.Writer, red redis.Redka) (any, error) {
	union := red.ZSet().UnionWith(cmd.Keys...).Dest(cmd.Dest)
	switch cmd.Aggregate {
	case sqlx.Min:
		union = union.Min()
	case sqlx.Max:
		union = union.Max()
	case sqlx.Sum:
		union = union.Sum()
	}

	count, err := union.Store()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	w.WriteInt(count)
	return count, nil
}
