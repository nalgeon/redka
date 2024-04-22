package command

import (
	"strconv"

	"github.com/nalgeon/redka/internal/parser"
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
	err := parser.New(
		parser.String(&cmd.key),
		parser.String(&cmd.field),
		parser.Float(&cmd.delta),
	).Required(3).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *HIncrByFloat) Run(w Writer, red Redka) (any, error) {
	val, err := red.Hash().IncrFloat(cmd.key, cmd.field, cmd.delta)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteBulkString(strconv.FormatFloat(val, 'f', -1, 64))
	return val, nil
}
