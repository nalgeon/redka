package command

import "strconv"

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
	if len(cmd.args) != 3 {
		return cmd, ErrInvalidArgNum
	}
	cmd.key = string(cmd.args[0])
	cmd.field = string(cmd.args[1])
	delta, err := strconv.Atoi(string(cmd.args[2]))
	if err != nil {
		return cmd, ErrInvalidInt
	}
	cmd.delta = delta
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
