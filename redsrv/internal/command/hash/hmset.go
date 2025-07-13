package hash

import (
	"github.com/nalgeon/redka/redsrv/internal/parser"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

// Sets the values of multiple fields in a hash.
// HMSET key field value [field value ...]
// https://redis.io/commands/hmset
type HMSet struct {
	redis.BaseCmd
	key   string
	items map[string]any
}

func ParseHMSet(b redis.BaseCmd) (HMSet, error) {
	cmd := HMSet{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.AnyMap(&cmd.items),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return HMSet{}, err
	}
	return cmd, nil
}

func (cmd HMSet) Run(w redis.Writer, red redis.Redka) (any, error) {
	count, err := red.Hash().SetMany(cmd.key, cmd.items)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return count, nil
}
