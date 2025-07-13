package string

import (
	"github.com/nalgeon/redka/redsrv/internal/parser"
	"github.com/nalgeon/redka/redsrv/internal/redis"
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
	key   string
	delta int
}

func ParseIncrBy(b redis.BaseCmd, sign int) (IncrBy, error) {
	cmd := IncrBy{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.delta),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return IncrBy{}, err
	}
	cmd.delta *= sign
	return cmd, nil
}

func (cmd IncrBy) Run(w redis.Writer, red redis.Redka) (any, error) {
	val, err := red.Str().Incr(cmd.key, cmd.delta)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(val)
	return val, nil
}
