package zset

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Removes members in a sorted set within a range of scores.
// ZREMRANGEBYSCORE key min max
// https://redis.io/commands/zremrangebyscore
type ZRemRangeByScore struct {
	redis.BaseCmd
	key string
	min float64
	max float64
}

func ParseZRemRangeByScore(b redis.BaseCmd) (ZRemRangeByScore, error) {
	cmd := ZRemRangeByScore{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&cmd.min),
		parser.Float(&cmd.max),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return ZRemRangeByScore{}, err
	}
	return cmd, nil
}

func (cmd ZRemRangeByScore) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.ZSet().DeleteWith(cmd.key).ByScore(cmd.min, cmd.max).Run()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
