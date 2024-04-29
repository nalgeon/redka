package zset

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Returns the score of a member in a sorted set.
// ZSCORE key member
// https://redis.io/commands/zscore
type ZScore struct {
	redis.BaseCmd
	Key    string
	Member string
}

func ParseZScore(b redis.BaseCmd) (*ZScore, error) {
	cmd := &ZScore{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.Key),
		parser.String(&cmd.Member),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZScore) Run(w redis.Writer, red redis.Redka) (any, error) {
	score, err := red.ZSet().GetScore(cmd.Key, cmd.Member)
	if err == core.ErrNotFound {
		w.WriteNull()
		return nil, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	redis.WriteFloat(w, score)
	return score, nil
}
