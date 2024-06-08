package set

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

// Returns a random member from a set after removing it.
// SPOP key
// https://redis.io/commands/spop
type SPop struct {
	redis.BaseCmd
	key string
}

func ParseSPop(b redis.BaseCmd) (SPop, error) {
	cmd := SPop{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return SPop{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd SPop) Run(w redis.Writer, red redis.Redka) (any, error) {
	elem, err := red.Set().Pop(cmd.key)
	if err == core.ErrNotFound {
		w.WriteNull()
		return elem, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteBulk(elem)
	return elem, nil
}
