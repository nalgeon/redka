package set

import "github.com/nalgeon/redka/internal/redis"

// Returns all members of a set.
// SMEMBERS key
// https://redis.io/commands/smembers
type SMembers struct {
	redis.BaseCmd
	key string
}

func ParseSMembers(b redis.BaseCmd) (SMembers, error) {
	cmd := SMembers{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return SMembers{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd SMembers) Run(w redis.Writer, red redis.Redka) (any, error) {
	items, err := red.Set().Items(cmd.key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteArray(len(items))
	for _, val := range items {
		w.WriteBulk(val)
	}
	return items, nil
}
