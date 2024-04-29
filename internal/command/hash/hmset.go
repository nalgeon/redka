package hash

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Sets the values of multiple fields in a hash.
// HMSET key field value [field value ...]
// https://redis.io/commands/hmset
type HMSet struct {
	redis.BaseCmd
	Key   string
	Items map[string]any
}

func ParseHMSet(b redis.BaseCmd) (*HMSet, error) {
	cmd := &HMSet{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.Key),
		parser.AnyMap(&cmd.Items),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *HMSet) Run(w redis.Writer, red redis.Redka) (any, error) {
	count, err := red.Hash().SetMany(cmd.Key, cmd.Items)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return count, nil
}
