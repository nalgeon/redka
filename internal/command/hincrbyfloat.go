package command

import (
	"strconv"

	"github.com/nalgeon/redka"
)

// Increments the floating point value of a field by a number.
// Uses 0 as initial value if the field doesn't exist.
// HINCRBY key field increment
// https://redis.io/commands/hincrbyfloat
type HIncrByFloat struct {
	baseCmd
	key   string
	field string
	delta float64
}

func parseHIncrByFloat(b baseCmd) (*HIncrByFloat, error) {
	cmd := &HIncrByFloat{baseCmd: b}
	if len(cmd.args) != 3 {
		return cmd, ErrInvalidArgNum
	}
	cmd.key = string(cmd.args[0])
	cmd.field = string(cmd.args[1])
	delta, err := strconv.ParseFloat(string(cmd.args[2]), 64)
	if err != nil {
		return cmd, ErrInvalidFloat
	}
	cmd.delta = delta
	return cmd, nil
}

func (cmd *HIncrByFloat) Run(w Writer, red *redka.Tx) (any, error) {
	val, err := red.Hash().IncrFloat(cmd.key, cmd.field, cmd.delta)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteBulkString(strconv.FormatFloat(val, 'f', -1, 64))
	return val, nil
}
