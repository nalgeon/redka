package string

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Atomically creates or modifies the string values of one or more keys.
// MSET key value [key value ...]
// https://redis.io/commands/mset
type MSet struct {
	redis.BaseCmd
	items map[string]any
}

func ParseMSet(b redis.BaseCmd) (MSet, error) {
	cmd := MSet{BaseCmd: b}
	err := parser.New(
		parser.AnyMap(&cmd.items),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return MSet{}, err
	}
	return cmd, nil
}

func (cmd MSet) Run(w redis.Writer, red redis.Redka) (any, error) {
	err := red.Str().SetMany(cmd.items)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return true, nil
}
