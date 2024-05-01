package list

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Prepends an element to a list.
// Creates the key if it doesn't exist.
// LPUSH key element
// https://redis.io/commands/lpush
type LPush struct {
	redis.BaseCmd
	key  string
	elem []byte
}

func ParseLPush(b redis.BaseCmd) (*LPush, error) {
	cmd := &LPush{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Bytes(&cmd.elem),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *LPush) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.List().PushFront(cmd.key, cmd.elem)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
