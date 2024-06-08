package set

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Returns the union of multiple sets.
// SUNION key [key ...]
// https://redis.io/commands/sunion
type SUnion struct {
	redis.BaseCmd
	keys []string
}

func ParseSUnion(b redis.BaseCmd) (SUnion, error) {
	cmd := SUnion{BaseCmd: b}
	err := parser.New(
		parser.Strings(&cmd.keys),
	).Required(1).Run(cmd.Args())
	if err != nil {
		return SUnion{}, err
	}
	return cmd, nil
}

func (cmd SUnion) Run(w redis.Writer, red redis.Redka) (any, error) {
	elems, err := red.Set().Union(cmd.keys...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteArray(len(elems))
	for _, elem := range elems {
		w.WriteBulk(elem)
	}
	return elems, nil
}
