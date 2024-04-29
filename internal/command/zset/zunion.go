package zset

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/sqlx"
)

// Returns the union of multiple sorted sets.
// ZUNION numkeys key [key ...] [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]
// https://redis.io/commands/zunion
type ZUnion struct {
	redis.BaseCmd
	Keys       []string
	Aggregate  string
	WithScores bool
}

func ParseZUnion(b redis.BaseCmd) (*ZUnion, error) {
	cmd := &ZUnion{BaseCmd: b}
	var nKeys int
	err := parser.New(
		parser.Int(&nKeys),
		parser.StringsN(&cmd.Keys, &nKeys),
		parser.Named("aggregate", parser.Enum(&cmd.Aggregate, sqlx.Sum, sqlx.Min, sqlx.Max)),
		parser.Flag("withscores", &cmd.WithScores),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZUnion) Run(w redis.Writer, red redis.Redka) (any, error) {
	union := red.ZSet().UnionWith(cmd.Keys...)
	switch cmd.Aggregate {
	case sqlx.Min:
		union = union.Min()
	case sqlx.Max:
		union = union.Max()
	case sqlx.Sum:
		union = union.Sum()
	}

	items, err := union.Run()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	if cmd.WithScores {
		w.WriteArray(len(items) * 2)
		for _, item := range items {
			w.WriteBulk(item.Elem)
			redis.WriteFloat(w, item.Score)
		}
	} else {
		w.WriteArray(len(items))
		for _, item := range items {
			w.WriteBulk(item.Elem)
		}
	}

	return items, nil
}
