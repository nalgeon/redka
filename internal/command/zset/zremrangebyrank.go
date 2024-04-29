package zset

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Removes members in a sorted set within a range of indexes.
// ZREMRANGEBYRANK key start stop
// https://redis.io/commands/zremrangebyrank
type ZRemRangeByRank struct {
	redis.BaseCmd
	Key   string
	Start int
	Stop  int
}

func ParseZRemRangeByRank(b redis.BaseCmd) (*ZRemRangeByRank, error) {
	cmd := &ZRemRangeByRank{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.Key),
		parser.Int(&cmd.Start),
		parser.Int(&cmd.Stop),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZRemRangeByRank) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.ZSet().DeleteWith(cmd.Key).ByRank(cmd.Start, cmd.Stop).Run()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
