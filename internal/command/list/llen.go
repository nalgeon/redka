package list

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Returns the length of a list.
// LLEN key
// https://redis.io/commands/llen
type LLen struct {
	redis.BaseCmd
	key string
}

func ParseLLen(b redis.BaseCmd) (LLen, error) {
	cmd := LLen{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
	).Required(1).Run(cmd.Args())
	if err != nil {
		return LLen{}, err
	}
	return cmd, nil
}

func (cmd LLen) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.List().Len(cmd.key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
