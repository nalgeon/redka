package zset

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Returns members in a sorted set within a range of indexes in reverse order.
// ZREVRANGE key start stop [WITHSCORES]
// https://redis.io/commands/zrevrange
type ZRevRange struct {
	redis.BaseCmd
	Key        string
	Start      int
	Stop       int
	WithScores bool
}

func ParseZRevRange(b redis.BaseCmd) (*ZRevRange, error) {
	cmd := &ZRevRange{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.Key),
		parser.Int(&cmd.Start),
		parser.Int(&cmd.Stop),
		parser.Flag("withscores", &cmd.WithScores),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZRevRange) Run(w redis.Writer, red redis.Redka) (any, error) {
	items, err := red.ZSet().RangeWith(cmd.Key).ByRank(cmd.Start, cmd.Stop).Desc().Run()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	// write the response with/without scores
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
