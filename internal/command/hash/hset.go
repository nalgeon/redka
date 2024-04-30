package hash

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Sets the values of one ore more fields in a hash.
// HSET key field value [field value ...]
// https://redis.io/commands/hset
type HSet struct {
	redis.BaseCmd
	key   string
	items map[string]any
}

func ParseHSet(b redis.BaseCmd) (*HSet, error) {
	cmd := &HSet{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.AnyMap(&cmd.items),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *HSet) Run(w redis.Writer, red redis.Redka) (any, error) {
	count, err := red.Hash().SetMany(cmd.key, cmd.items)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
