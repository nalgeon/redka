package zset

import (
	"github.com/nalgeon/redka/redsrv/internal/parser"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

// Returns members in a sorted set within a range of scores.
// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
// https://redis.io/commands/zrangebyscore
type ZRangeByScore struct {
	redis.BaseCmd
	key        string
	min        float64
	max        float64
	withScores bool
	offset     int
	count      int
}

func ParseZRangeByScore(b redis.BaseCmd) (ZRangeByScore, error) {
	cmd := ZRangeByScore{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&cmd.min),
		parser.Float(&cmd.max),
		parser.Flag("withscores", &cmd.withScores),
		parser.Named("limit", parser.Int(&cmd.offset), parser.Int(&cmd.count)),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return ZRangeByScore{}, err
	}
	return cmd, nil
}

func (cmd ZRangeByScore) Run(w redis.Writer, red redis.Redka) (any, error) {
	rang := red.ZSet().RangeWith(cmd.key).ByScore(cmd.min, cmd.max)

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
