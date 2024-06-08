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
	key        string
	start      float64
	stop       float64
	byScore    bool
	rev        bool
	offset     int
	count      int
	withScores bool
}

func ParseZRange(b redis.BaseCmd) (ZRange, error) {
	cmd := ZRange{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&cmd.start),
		parser.Float(&cmd.stop),
		parser.Flag("byscore", &cmd.byScore),
		parser.Flag("rev", &cmd.rev),
		parser.Named("limit", parser.Int(&cmd.offset), parser.Int(&cmd.count)),
		parser.Flag("withscores", &cmd.withScores),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return ZRange{}, err
	}
	return cmd, nil
}

func (cmd ZRange) Run(w redis.Writer, red redis.Redka) (any, error) {
	rang := red.ZSet().RangeWith(cmd.key)

	// filter by score or rank
	if cmd.byScore {
		rang = rang.ByScore(cmd.start, cmd.stop)
	} else {
		rang = rang.ByRank(int(cmd.start), int(cmd.stop))
	}

	// sort direction
	if cmd.rev {
		rang = rang.Desc()
	}

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
