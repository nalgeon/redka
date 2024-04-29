package key

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

// Removes the expiration time of a key.
// PERSIST key
// https://redis.io/commands/persist
type Persist struct {
	redis.BaseCmd
	Key string
}

func ParsePersist(b redis.BaseCmd) (*Persist, error) {
	cmd := &Persist{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return cmd, redis.ErrInvalidArgNum
	}
	cmd.Key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd *Persist) Run(w redis.Writer, red redis.Redka) (any, error) {
	err := red.Key().Persist(cmd.Key)
	if err != nil && err != core.ErrNotFound {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if err == core.ErrNotFound {
		w.WriteInt(0)
		return false, nil

	}
	w.WriteInt(1)
	return true, nil
}
