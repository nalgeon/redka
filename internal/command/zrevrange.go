package command

import (
	"github.com/nalgeon/redka/internal/parser"
)

// Returns members in a sorted set within a range of indexes in reverse order.
// ZREVRANGE key start stop [WITHSCORES]
// https://redis.io/commands/zrevrange
type ZRevRange struct {
	baseCmd
	key        string
	start      int
	stop       int
	withScores bool
}

func parseZRevRange(b baseCmd) (*ZRevRange, error) {
	cmd := &ZRevRange{baseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.start),
		parser.Int(&cmd.stop),
		parser.Flag("withscores", &cmd.withScores),
	).Required(3).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZRevRange) Run(w Writer, red Redka) (any, error) {
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
