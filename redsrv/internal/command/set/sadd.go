package set

import (
	"github.com/nalgeon/redka/redsrv/internal/parser"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

// Adds one or more members to a set.
// Creates the key if it doesn't exist.
// SADD key member [member ...]
// https://redis.io/commands/sadd
type SAdd struct {
	redis.BaseCmd
	key     string
	members []any
}

func ParseSAdd(b redis.BaseCmd) (SAdd, error) {
	cmd := SAdd{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Anys(&cmd.members),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return SAdd{}, err
	}
	return cmd, nil
}

func (cmd SAdd) Run(w redis.Writer, red redis.Redka) (any, error) {
	count, err := red.Set().Add(cmd.key, cmd.members...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
