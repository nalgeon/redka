package command

import (
	"github.com/nalgeon/redka/internal/parser"
)

// Removes members in a sorted set within a range of scores.
// ZREMRANGEBYSCORE key min max
// https://redis.io/commands/zremrangebyscore
type ZRemRangeByScore struct {
	baseCmd
	key string
	min float64
	max float64
}

func parseZRemRangeByScore(b baseCmd) (*ZRemRangeByScore, error) {
	cmd := &ZRemRangeByScore{baseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&cmd.min),
		parser.Float(&cmd.max),
	).Required(3).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZRemRangeByScore) Run(w Writer, red Redka) (any, error) {
	n, err := red.ZSet().DeleteWith(cmd.key).ByScore(cmd.min, cmd.max).Run()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return 0, err
	}
	w.WriteInt(n)
	return n, nil
}
