package command

import "strconv"

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
	if len(cmd.args) != 2 {
		return cmd, ErrInvalidArgNum
	}
	var err error
	cmd.key = string(cmd.args[0])
	cmd.delta, err = strconv.Atoi(string(cmd.args[1]))
	if err != nil {
		return cmd, ErrInvalidInt
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
