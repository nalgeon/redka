package command

import (
	"github.com/nalgeon/redka/internal/parser"
)

// Returns members in a sorted set within a range of indexes.
// ZRANGE key start stop [BYSCORE] [REV] [LIMIT offset count] [WITHSCORES]
// https://redis.io/commands/zrange
type ZRange struct {
	baseCmd
	key        string
	start      float64
	stop       float64
	byScore    bool
	rev        bool
	offset     int
	count      int
	withScores bool
}

func parseZRange(b baseCmd) (*ZRange, error) {
	cmd := &ZRange{baseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&cmd.start),
		parser.Float(&cmd.stop),
		parser.Flag("byscore", &cmd.byScore),
		parser.Flag("rev", &cmd.rev),
		parser.Named("limit", parser.Int(&cmd.offset), parser.Int(&cmd.count)),
		parser.Flag("withscores", &cmd.withScores),
	).Required(3).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZRange) Run(w Writer, red Redka) (any, error) {
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
		return items, err
	}

	// write the response with/without scores
	if cmd.withScores {
		w.WriteArray(len(items) * 2)
		for _, item := range items {
			w.WriteBulk(item.Elem)
			writeFloat(w, item.Score)
		}
	} else {
		w.WriteArray(len(items))
		for _, item := range items {
			w.WriteBulk(item.Elem)
		}
	}

	return items, nil
}
