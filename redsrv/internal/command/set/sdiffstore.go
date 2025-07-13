package set

import (
	"github.com/nalgeon/redka/redsrv/internal/parser"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

// Stores the difference of multiple sets in a key.
// SDIFFSTORE destination key [key ...]
// https://redis.io/commands/sdiffstore
type SDiffStore struct {
	redis.BaseCmd
	dest string
	keys []string
}

func ParseSDiffStore(b redis.BaseCmd) (SDiffStore, error) {
	cmd := SDiffStore{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.dest),
		parser.Strings(&cmd.keys),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return SDiffStore{}, err
	}
	return cmd, nil
}

func (cmd SDiffStore) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.Set().DiffStore(cmd.dest, cmd.keys...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
