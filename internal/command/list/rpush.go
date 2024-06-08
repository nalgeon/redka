package list

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Appends an element to a list.
// Creates the key if it doesn't exist.
// RPUSH key element
// https://redis.io/commands/rpush
type RPush struct {
	redis.BaseCmd
	key  string
	elem []byte
}

func ParseRPush(b redis.BaseCmd) (RPush, error) {
	cmd := RPush{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Bytes(&cmd.elem),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return RPush{}, err
	}
	return cmd, nil
}

func (cmd RPush) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.List().PushBack(cmd.key, cmd.elem)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
