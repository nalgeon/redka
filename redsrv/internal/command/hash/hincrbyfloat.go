package hash

import (
	"github.com/nalgeon/redka/redsrv/internal/parser"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

// Increments the floating point value of a field by a number.
// Uses 0 as initial value if the field doesn't exist.
// HINCRBY key field increment
// https://redis.io/commands/hincrbyfloat
type HIncrByFloat struct {
	redis.BaseCmd
	key   string
	field string
	delta float64
}

func ParseHIncrByFloat(b redis.BaseCmd) (HIncrByFloat, error) {
	cmd := HIncrByFloat{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.String(&cmd.field),
		parser.Float(&cmd.delta),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return HIncrByFloat{}, err
	}
	return cmd, nil
}

func (cmd HIncrByFloat) Run(w redis.Writer, red redis.Redka) (any, error) {
	val, err := red.Hash().IncrFloat(cmd.key, cmd.field, cmd.delta)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	redis.WriteFloat(w, val)
	return val, nil
}
