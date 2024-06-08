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
	key        string
	min        float64
	max        float64
	withScores bool
	offset     int
	count      int
}

func ParseZRevRangeByScore(b redis.BaseCmd) (ZRevRangeByScore, error) {
	cmd := ZRevRangeByScore{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&cmd.min),
		parser.Float(&cmd.max),
		parser.Flag("withscores", &cmd.withScores),
		parser.Named("limit", parser.Int(&cmd.offset), parser.Int(&cmd.count)),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return ZRevRangeByScore{}, err
	}
	return cmd, nil
}

func (cmd ZRevRangeByScore) Run(w redis.Writer, red redis.Redka) (any, error) {
	rang := red.ZSet().RangeWith(cmd.key).ByScore(cmd.min, cmd.max).Desc()

	// limit and offset
	if cmd.offset > 0 {
		rang = rang.Offset(cmd.offset)
	}
	if cmd.count > 0 {
		rang = rang.Count(cmd.count)
	}

	// run the command
	items, err := rang.Run()
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
