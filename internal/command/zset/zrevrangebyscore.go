package zset

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Returns members in a sorted set within a range of scores in reverse order.
// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
// https://redis.io/commands/zrangebyscore
type ZRevRangeByScore struct {
	redis.BaseCmd
	Key        string
	Min        float64
	Max        float64
	WithScores bool
	Offset     int
	Count      int
}

func ParseZRevRangeByScore(b redis.BaseCmd) (*ZRevRangeByScore, error) {
	cmd := &ZRevRangeByScore{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.Key),
		parser.Float(&cmd.Min),
		parser.Float(&cmd.Max),
		parser.Flag("withscores", &cmd.WithScores),
		parser.Named("limit", parser.Int(&cmd.Offset), parser.Int(&cmd.Count)),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZRevRangeByScore) Run(w redis.Writer, red redis.Redka) (any, error) {
	rang := red.ZSet().RangeWith(cmd.Key).ByScore(cmd.Min, cmd.Max).Desc()

	// limit and offset
	if cmd.Offset > 0 {
		rang = rang.Offset(cmd.Offset)
	}
	if cmd.Count > 0 {
		rang = rang.Count(cmd.Count)
	}

	// run the command
	items, err := rang.Run()
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
