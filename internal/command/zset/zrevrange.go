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
	key        string
	start      int
	stop       int
	withScores bool
}

func ParseZRevRange(b redis.BaseCmd) (*ZRevRange, error) {
	cmd := &ZRevRange{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.start),
		parser.Int(&cmd.stop),
		parser.Flag("withscores", &cmd.withScores),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZRevRange) Run(w redis.Writer, red redis.Redka) (any, error) {
	items, err := red.ZSet().RangeWith(cmd.key).ByRank(cmd.start, cmd.stop).Desc().Run()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	// write the response with/without scores
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
