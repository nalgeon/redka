package set

import "github.com/nalgeon/redka/internal/redis"

// Returns the number of members in a set.
// SCARD key
// https://redis.io/commands/scard
type SCard struct {
	redis.BaseCmd
	key string
}

func ParseSCard(b redis.BaseCmd) (*SCard, error) {
	cmd := &SCard{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return cmd, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd *SCard) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.Set().Len(cmd.key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
