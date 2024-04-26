package command

import "github.com/nalgeon/redka/internal/parser"

// Returns the count of members in a sorted set that have scores within a range.
// ZCOUNT key min max
// https://redis.io/commands/zcount
type ZCount struct {
	baseCmd
	key string
	min float64
	max float64
}

func parseZCount(b baseCmd) (*ZCount, error) {
	cmd := &ZCount{baseCmd: b}
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

func (cmd *ZCount) Run(w Writer, red Redka) (any, error) {
	n, err := red.ZSet().Count(cmd.key, cmd.min, cmd.max)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
