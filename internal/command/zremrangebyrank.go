package command

import (
	"github.com/nalgeon/redka/internal/parser"
)

// Removes members in a sorted set within a range of indexes.
// ZREMRANGEBYRANK key start stop
// https://redis.io/commands/zremrangebyrank
type ZRemRangeByRank struct {
	baseCmd
	key   string
	start int
	stop  int
}

func parseZRemRangeByRank(b baseCmd) (*ZRemRangeByRank, error) {
	cmd := &ZRemRangeByRank{baseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.start),
		parser.Int(&cmd.stop),
	).Required(3).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZRemRangeByRank) Run(w Writer, red Redka) (any, error) {
	n, err := red.ZSet().DeleteWith(cmd.key).ByRank(cmd.start, cmd.stop).Run()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
