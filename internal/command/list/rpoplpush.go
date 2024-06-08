package list

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Returns the last element of a list after removing
// and pushing it to another list.
// RPOPLPUSH source destination
// https://redis.io/commands/rpoplpush
type RPopLPush struct {
	redis.BaseCmd
	src string
	dst string
}

func ParseRPopLPush(b redis.BaseCmd) (RPopLPush, error) {
	cmd := RPopLPush{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.src),
		parser.String(&cmd.dst),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return RPopLPush{}, err
	}
	return cmd, nil
}

func (cmd RPopLPush) Run(w redis.Writer, red redis.Redka) (any, error) {
	val, err := red.List().PopBackPushFront(cmd.src, cmd.dst)
	if err == core.ErrNotFound {
		w.WriteNull()
		return val, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteBulk(val)
	return val, nil
}
