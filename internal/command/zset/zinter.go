package zset

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/sqlx"
)

// Returns the intersect of multiple sorted sets.
// ZINTER numkeys key [key ...] [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]
// https://redis.io/commands/zinter
type ZInter struct {
	redis.BaseCmd
	keys       []string
	aggregate  string
	withScores bool
}

func ParseZInter(b redis.BaseCmd) (ZInter, error) {
	cmd := ZInter{BaseCmd: b}
	var nKeys int
	err := parser.New(
		parser.Int(&nKeys),
		parser.StringsN(&cmd.keys, &nKeys),
		parser.Named("aggregate", parser.Enum(&cmd.aggregate, sqlx.Sum, sqlx.Min, sqlx.Max)),
		parser.Flag("withscores", &cmd.withScores),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return ZInter{}, err
	}
	return cmd, nil
}

func (cmd ZInter) Run(w redis.Writer, red redis.Redka) (any, error) {
	inter := red.ZSet().InterWith(cmd.keys...)
	switch cmd.aggregate {
	case sqlx.Min:
		inter = inter.Min()
	case sqlx.Max:
		inter = inter.Max()
	case sqlx.Sum:
		inter = inter.Sum()
	}

	items, err := inter.Run()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	if cmd.withScores {
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
