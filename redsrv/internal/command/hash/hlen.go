package hash

import "github.com/nalgeon/redka/redsrv/internal/redis"

// Returns the number of fields in a hash.
// HLEN key
// https://redis.io/commands/hlen
type HLen struct {
	redis.BaseCmd
	key string
}

func ParseHLen(b redis.BaseCmd) (HLen, error) {
	cmd := HLen{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return HLen{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd HLen) Run(w redis.Writer, red redis.Redka) (any, error) {
	count, err := red.Hash().Len(cmd.key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
