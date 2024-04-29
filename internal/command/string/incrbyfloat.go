package string

import (
	"strconv"

	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Increment the floating point value of a key by a number.
// Uses 0 as initial value if the key doesn't exist.
// INCRBYFLOAT key increment
// https://redis.io/commands/incrbyfloat
type IncrByFloat struct {
	redis.BaseCmd
	Key   string
	Delta float64
}

func ParseIncrByFloat(b redis.BaseCmd) (*IncrByFloat, error) {
	cmd := &IncrByFloat{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.Key),
		parser.Float(&cmd.Delta),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *IncrByFloat) Run(w redis.Writer, red redis.Redka) (any, error) {
	val, err := red.Str().IncrFloat(cmd.Key, cmd.Delta)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteBulkString(strconv.FormatFloat(val, 'f', -1, 64))
	return val, nil
}
