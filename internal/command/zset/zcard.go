package zset

import "github.com/nalgeon/redka/internal/redis"

// Returns the number of members in a sorted set.
// ZCARD key
// https://redis.io/commands/zcard
type ZCard struct {
	redis.BaseCmd
	Key string
}

func ParseZCard(b redis.BaseCmd) (*ZCard, error) {
	cmd := &ZCard{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return cmd, redis.ErrInvalidArgNum
	}
	cmd.Key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd *ZCard) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.ZSet().Len(cmd.Key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
