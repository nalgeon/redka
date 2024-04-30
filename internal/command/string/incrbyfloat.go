package string

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Increment the floating point value of a key by a number.
// Uses 0 as initial value if the key doesn't exist.
// INCRBYFLOAT key increment
// https://redis.io/commands/incrbyfloat
type IncrByFloat struct {
	redis.BaseCmd
	key   string
	delta float64
}

func ParseIncrByFloat(b redis.BaseCmd) (*IncrByFloat, error) {
	cmd := &IncrByFloat{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&cmd.delta),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *IncrByFloat) Run(w redis.Writer, red redis.Redka) (any, error) {
	val, err := red.Str().IncrFloat(cmd.key, cmd.delta)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	redis.WriteFloat(w, val)
	return val, nil
}
