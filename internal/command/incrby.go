package command

import "github.com/nalgeon/redka/internal/parser"

// Increments the integer value of a key by a number.
// Uses 0 as initial value if the key doesn't exist.
// INCRBY key increment
// https://redis.io/commands/incrby
//
// Decrements the integer value of a key by a number.
// Uses 0 as initial value if the key doesn't exist.
// DECRBY key increment
// https://redis.io/commands/decrby
type IncrBy struct {
	baseCmd
	key   string
	delta int
}

func parseIncrBy(b baseCmd, sign int) (*IncrBy, error) {
	cmd := &IncrBy{baseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.delta),
	).Required(2).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	cmd.delta *= sign
	return cmd, nil
}

func (cmd *IncrBy) Run(w Writer, red Redka) (any, error) {
	val, err := red.Str().Incr(cmd.key, cmd.delta)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(val)
	return val, nil
}
