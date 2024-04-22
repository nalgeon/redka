package command

import "github.com/nalgeon/redka/internal/parser"

// Increments the integer value of a field in a hash by a number.
// Uses 0 as initial value if the field doesn't exist.
// HINCRBY key field increment
// https://redis.io/commands/hincrby
type HIncrBy struct {
	baseCmd
	key   string
	field string
	delta int
}

func parseHIncrBy(b baseCmd) (*HIncrBy, error) {
	cmd := &HIncrBy{baseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.String(&cmd.field),
		parser.Int(&cmd.delta),
	).Required(3).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *HIncrBy) Run(w Writer, red Redka) (any, error) {
	val, err := red.Hash().Incr(cmd.key, cmd.field, cmd.delta)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(val)
	return val, nil
}
