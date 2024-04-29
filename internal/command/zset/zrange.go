package zset

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Returns members in a sorted set within a range of indexes.
// ZRANGE key start stop [BYSCORE] [REV] [LIMIT offset count] [WITHSCORES]
// https://redis.io/commands/zrange
type ZRange struct {
	redis.BaseCmd
	Key        string
	Start      float64
	Stop       float64
	ByScore    bool
	Rev        bool
	Offset     int
	Count      int
	WithScores bool
}

func ParseZRange(b redis.BaseCmd) (*ZRange, error) {
	cmd := &ZRange{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.Key),
		parser.Float(&cmd.Start),
		parser.Float(&cmd.Stop),
		parser.Flag("byscore", &cmd.ByScore),
		parser.Flag("rev", &cmd.Rev),
		parser.Named("limit", parser.Int(&cmd.Offset), parser.Int(&cmd.Count)),
		parser.Flag("withscores", &cmd.WithScores),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZRange) Run(w redis.Writer, red redis.Redka) (any, error) {
	rang := red.ZSet().RangeWith(cmd.Key)

	// filter by score or rank
	if cmd.ByScore {
		rang = rang.ByScore(cmd.Start, cmd.Stop)
	} else {
		rang = rang.ByRank(int(cmd.Start), int(cmd.Stop))
	}

	// sort direction
	if cmd.Rev {
		rang = rang.Desc()
	}

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
