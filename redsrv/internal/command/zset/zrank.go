package zset

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/parser"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

// Returns the index of a member in a sorted set ordered by ascending scores.
// ZRANK key member [WITHSCORE]
// https://redis.io/commands/zrank
type ZRank struct {
	redis.BaseCmd
	key       string
	member    string
	withScore bool
}

func ParseZRank(b redis.BaseCmd) (ZRank, error) {
	cmd := ZRank{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.String(&cmd.member),
		parser.Flag("withscore", &cmd.withScore),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return ZRank{}, err
	}
	return cmd, nil
}

func (cmd ZRank) Run(w redis.Writer, red redis.Redka) (any, error) {
	rank, score, err := red.ZSet().GetRank(cmd.key, cmd.member)
	if err == core.ErrNotFound {
		w.WriteNull()
		return nil, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if cmd.withScore {
		w.WriteArray(2)
		w.WriteInt(rank)
		redis.WriteFloat(w, score)
		return rank, nil
	}
	w.WriteInt(rank)
	return rank, nil
}
