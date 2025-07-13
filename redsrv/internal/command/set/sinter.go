package set

import (
	"github.com/nalgeon/redka/redsrv/internal/parser"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

// Returns the intersect of multiple sets.
// SINTER key [key ...]
// https://redis.io/commands/sinter
type SInter struct {
	redis.BaseCmd
	keys []string
}

func ParseSInter(b redis.BaseCmd) (SInter, error) {
	cmd := SInter{BaseCmd: b}
	err := parser.New(
		parser.Strings(&cmd.keys),
	).Required(1).Run(cmd.Args())
	if err != nil {
		return SInter{}, err
	}
	return cmd, nil
}

func (cmd SInter) Run(w redis.Writer, red redis.Redka) (any, error) {
	elems, err := red.Set().Inter(cmd.keys...)
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
