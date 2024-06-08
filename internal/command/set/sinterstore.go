package set

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Stores the intersect of multiple sets in a key.
// SINTERSTORE destination key [key ...]
// https://redis.io/commands/sinterstore
type SInterStore struct {
	redis.BaseCmd
	dest string
	keys []string
}

func ParseSInterStore(b redis.BaseCmd) (SInterStore, error) {
	cmd := SInterStore{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.dest),
		parser.Strings(&cmd.keys),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return SInterStore{}, err
	}
	return cmd, nil
}

func (cmd SInterStore) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.Set().InterStore(cmd.dest, cmd.keys...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
