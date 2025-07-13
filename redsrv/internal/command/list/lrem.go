package list

import (
	"github.com/nalgeon/redka/redsrv/internal/parser"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

// Removes elements from a list.
// LREM key count element
// https://redis.io/commands/lrem
type LRem struct {
	redis.BaseCmd
	key   string
	count int
	elem  []byte
}

func ParseLRem(b redis.BaseCmd) (LRem, error) {
	cmd := LRem{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.count),
		parser.Bytes(&cmd.elem),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return LRem{}, err
	}
	return cmd, nil
}

func (cmd LRem) Run(w redis.Writer, red redis.Redka) (any, error) {
	var n int
	var err error
	switch {
	case cmd.count > 0:
		n, err = red.List().DeleteFront(cmd.key, cmd.elem, cmd.count)
	case cmd.count < 0:
		n, err = red.List().DeleteBack(cmd.key, cmd.elem, -cmd.count)
	case cmd.count == 0:
		n, err = red.List().Delete(cmd.key, cmd.elem)
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
