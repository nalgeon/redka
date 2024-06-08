package zset

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Increments the score of a member in a sorted set.
// ZINCRBY key increment member
// https://redis.io/commands/zincrby
type ZIncrBy struct {
	redis.BaseCmd
	key    string
	delta  float64
	member string
}

func ParseZIncrBy(b redis.BaseCmd) (ZIncrBy, error) {
	cmd := ZIncrBy{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&cmd.delta),
		parser.String(&cmd.member),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return ZIncrBy{}, err
	}
	return cmd, nil
}

func (cmd ZIncrBy) Run(w redis.Writer, red redis.Redka) (any, error) {
	score, err := red.ZSet().Incr(cmd.key, cmd.member, cmd.delta)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	redis.WriteFloat(w, score)
	return score, nil
}
