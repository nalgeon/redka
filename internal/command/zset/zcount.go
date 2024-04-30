package zset

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Returns the count of members in a sorted set that have scores within a range.
// ZCOUNT key min max
// https://redis.io/commands/zcount
type ZCount struct {
	redis.BaseCmd
	key string
	min float64
	max float64
}

func ParseZCount(b redis.BaseCmd) (*ZCount, error) {
	cmd := &ZCount{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&cmd.min),
		parser.Float(&cmd.max),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZCount) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.ZSet().Count(cmd.key, cmd.min, cmd.max)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
