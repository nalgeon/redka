package set

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

// Get a random member from a set.
// SRANDMEMBER key
// https://redis.io/commands/srandmember
type SRandMember struct {
	redis.BaseCmd
	key string
}

func ParseSRandMember(b redis.BaseCmd) (SRandMember, error) {
	cmd := SRandMember{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return SRandMember{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd SRandMember) Run(w redis.Writer, red redis.Redka) (any, error) {
	elem, err := red.Set().Random(cmd.key)
	if err == core.ErrNotFound {
		w.WriteNull()
		return elem, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteBulk(elem)
	return elem, nil
}
