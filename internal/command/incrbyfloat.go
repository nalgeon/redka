package command

import "strconv"

// Increment the floating point value of a key by a number.
// Uses 0 as initial value if the key doesn't exist.
// INCRBYFLOAT key increment
// https://redis.io/commands/incrbyfloat
type IncrByFloat struct {
	baseCmd
	key   string
	delta float64
}

func parseIncrByFloat(b baseCmd) (*IncrByFloat, error) {
	cmd := &IncrByFloat{baseCmd: b}
	if len(cmd.args) != 2 {
		return cmd, ErrInvalidArgNum
	}
	var err error
	cmd.key = string(cmd.args[0])
	cmd.delta, err = strconv.ParseFloat(string(cmd.args[1]), 64)
	if err != nil {
		return cmd, ErrInvalidFloat
	}
	return cmd, nil
}

func (cmd *IncrByFloat) Run(w Writer, red Redka) (any, error) {
	val, err := red.Str().IncrFloat(cmd.key, cmd.delta)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteBulkString(strconv.FormatFloat(val, 'f', -1, 64))
	return val, nil
}
