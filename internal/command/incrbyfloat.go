package command

import (
	"strconv"

	"github.com/nalgeon/redka/internal/parser"
)

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
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&cmd.delta),
	).Required(2).Run(cmd.args)
	if err != nil {
		return nil, err
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
