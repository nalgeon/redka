package list

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/parser"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

// Returns the last element of a list after removing it.
// RPOP key
// https://redis.io/commands/rpop
type RPop struct {
	redis.BaseCmd
	key string
}

func ParseRPop(b redis.BaseCmd) (RPop, error) {
	cmd := RPop{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
	).Required(1).Run(cmd.Args())
	if err != nil {
		return RPop{}, err
	}
	return cmd, nil
}

func (cmd RPop) Run(w redis.Writer, red redis.Redka) (any, error) {
	val, err := red.List().PopBack(cmd.key)
	if err == core.ErrNotFound {
		w.WriteNull()
		return val, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteBulk(val)
	return val, nil
}
