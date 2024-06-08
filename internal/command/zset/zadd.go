package zset

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Adds one or more members to a sorted set, or updates their scores.
// Creates the key if it doesn't exist.
// ZADD key score member [score member ...]
// https://redis.io/commands/zadd
type ZAdd struct {
	redis.BaseCmd
	key   string
	items map[any]float64
}

func ParseZAdd(b redis.BaseCmd) (ZAdd, error) {
	cmd := ZAdd{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.FloatMap(&cmd.items),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return ZAdd{}, err
	}
	return cmd, nil
}

func (cmd ZAdd) Run(w redis.Writer, red redis.Redka) (any, error) {
	count, err := red.ZSet().AddMany(cmd.key, cmd.items)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
