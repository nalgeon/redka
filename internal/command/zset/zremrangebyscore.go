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
	Key string
	Min float64
	Max float64
}

func ParseZRemRangeByScore(b redis.BaseCmd) (*ZRemRangeByScore, error) {
	cmd := &ZRemRangeByScore{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.Key),
		parser.Float(&cmd.Min),
		parser.Float(&cmd.Max),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZRemRangeByScore) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.ZSet().DeleteWith(cmd.Key).ByScore(cmd.Min, cmd.Max).Run()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
