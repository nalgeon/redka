package set

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Returns the difference of multiple sets.
// SDIFF key [key ...]
// https://redis.io/commands/sdiff
type SDiff struct {
	redis.BaseCmd
	keys []string
}

func ParseSDiff(b redis.BaseCmd) (*SDiff, error) {
	cmd := &SDiff{BaseCmd: b}
	err := parser.New(
		parser.Strings(&cmd.keys),
	).Required(1).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *SDiff) Run(w redis.Writer, red redis.Redka) (any, error) {
	elems, err := red.Set().Diff(cmd.keys...)
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
