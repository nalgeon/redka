package zset

import "github.com/nalgeon/redka/internal/redis"

// Returns the number of members in a sorted set.
// ZCARD key
// https://redis.io/commands/zcard
type ZCard struct {
	redis.BaseCmd
	key string
}

func ParseZCard(b redis.BaseCmd) (ZCard, error) {
	cmd := ZCard{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return ZCard{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd ZCard) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.ZSet().Len(cmd.key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
