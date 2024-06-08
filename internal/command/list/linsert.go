package list

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

const (
	Before = "before"
	After  = "after"
)

// Inserts an element before or after another element in a list.
// LINSERT key <BEFORE | AFTER> pivot element
// https://redis.io/commands/linsert
type LInsert struct {
	redis.BaseCmd
	key   string
	where string
	pivot []byte
	elem  []byte
}

func ParseLInsert(b redis.BaseCmd) (LInsert, error) {
	cmd := LInsert{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Enum(&cmd.where, Before, After),
		parser.Bytes(&cmd.pivot),
		parser.Bytes(&cmd.elem),
	).Required(4).Run(cmd.Args())
	if err != nil {
		return LInsert{}, err
	}
	return cmd, nil
}

func (cmd LInsert) Run(w redis.Writer, red redis.Redka) (any, error) {
	var n int
	var err error
	if cmd.where == Before {
		n, err = red.List().InsertBefore(cmd.key, cmd.pivot, cmd.elem)
	} else {
		n, err = red.List().InsertAfter(cmd.key, cmd.pivot, cmd.elem)
	}
	if err == core.ErrNotFound {
		w.WriteInt(n)
		return n, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
