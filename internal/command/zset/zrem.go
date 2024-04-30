package zset

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Removes one or more members from a sorted set.
// ZREM key member [member ...]
// https://redis.io/commands/zrem
type ZRem struct {
	redis.BaseCmd
	key     string
	members []any
}

func ParseZRem(b redis.BaseCmd) (*ZRem, error) {
	cmd := &ZRem{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Anys(&cmd.members),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZRem) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.ZSet().Delete(cmd.key, cmd.members...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
