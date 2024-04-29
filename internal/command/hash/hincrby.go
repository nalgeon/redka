package hash

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Increments the integer value of a field in a hash by a number.
// Uses 0 as initial value if the field doesn't exist.
// HINCRBY key field increment
// https://redis.io/commands/hincrby
type HIncrBy struct {
	redis.BaseCmd
	Key   string
	Field string
	Delta int
}

func ParseHIncrBy(b redis.BaseCmd) (*HIncrBy, error) {
	cmd := &HIncrBy{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.Key),
		parser.String(&cmd.Field),
		parser.Int(&cmd.Delta),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *HIncrBy) Run(w redis.Writer, red redis.Redka) (any, error) {
	val, err := red.Hash().Incr(cmd.Key, cmd.Field, cmd.Delta)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(val)
	return val, nil
}
