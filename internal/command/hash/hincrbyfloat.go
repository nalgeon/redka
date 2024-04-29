package hash

import (
	"strconv"

	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Increments the floating point value of a field by a number.
// Uses 0 as initial value if the field doesn't exist.
// HINCRBY key field increment
// https://redis.io/commands/hincrbyfloat
type HIncrByFloat struct {
	redis.BaseCmd
	Key   string
	Field string
	Delta float64
}

func ParseHIncrByFloat(b redis.BaseCmd) (*HIncrByFloat, error) {
	cmd := &HIncrByFloat{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.Key),
		parser.String(&cmd.Field),
		parser.Float(&cmd.Delta),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *HIncrByFloat) Run(w redis.Writer, red redis.Redka) (any, error) {
	val, err := red.Hash().IncrFloat(cmd.Key, cmd.Field, cmd.Delta)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteBulkString(strconv.FormatFloat(val, 'f', -1, 64))
	return val, nil
}
