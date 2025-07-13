package key

import (
	"github.com/nalgeon/redka/redsrv/internal/parser"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

// Determines whether one or more keys exist.
// EXISTS key [key ...]
// https://redis.io/commands/exists
type Exists struct {
	redis.BaseCmd
	keys []string
}

func ParseExists(b redis.BaseCmd) (Exists, error) {
	cmd := Exists{BaseCmd: b}
	err := parser.New(
		parser.Strings(&cmd.keys),
	).Required(1).Run(cmd.Args())
	if err != nil {
		return Exists{}, err
	}
	return cmd, nil
}

func (cmd Exists) Run(w redis.Writer, red redis.Redka) (any, error) {
	count, err := red.Key().Count(cmd.keys...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
