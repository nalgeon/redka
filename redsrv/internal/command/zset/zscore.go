package zset

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/parser"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

// Returns the score of a member in a sorted set.
// ZSCORE key member
// https://redis.io/commands/zscore
type ZScore struct {
	redis.BaseCmd
	key    string
	member string
}

func ParseZScore(b redis.BaseCmd) (ZScore, error) {
	cmd := ZScore{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.String(&cmd.member),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return ZScore{}, err
	}
	return cmd, nil
}

func (cmd ZScore) Run(w redis.Writer, red redis.Redka) (any, error) {
	score, err := red.ZSet().GetScore(cmd.key, cmd.member)
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
