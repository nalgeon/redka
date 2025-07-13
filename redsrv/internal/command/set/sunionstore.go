package set

import (
	"github.com/nalgeon/redka/redsrv/internal/parser"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

// Stores the union of multiple sets in a key.
// SUNIONSTORE destination key [key ...]
// https://redis.io/commands/sunionstore
type SUnionStore struct {
	redis.BaseCmd
	dest string
	keys []string
}

func ParseSUnionStore(b redis.BaseCmd) (SUnionStore, error) {
	cmd := SUnionStore{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.dest),
		parser.Strings(&cmd.keys),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return SUnionStore{}, err
	}
	return cmd, nil
}

func (cmd SUnionStore) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.Set().UnionStore(cmd.dest, cmd.keys...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
