package string

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

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
	redis.BaseCmd
	Key   string
	Delta int
}

func ParseIncrBy(b redis.BaseCmd, sign int) (*IncrBy, error) {
	cmd := &IncrBy{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.Key),
		parser.Int(&cmd.Delta),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	cmd.Delta *= sign
	return cmd, nil
}

func (cmd *IncrBy) Run(w redis.Writer, red redis.Redka) (any, error) {
	val, err := red.Str().Incr(cmd.Key, cmd.Delta)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(val)
	return val, nil
}
