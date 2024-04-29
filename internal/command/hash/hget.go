package hash

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

// Returns the value of a field in a hash.
// HGET key field
// https://redis.io/commands/hget
type HGet struct {
	redis.BaseCmd
	Key   string
	Field string
}

func ParseHGet(b redis.BaseCmd) (*HGet, error) {
	cmd := &HGet{BaseCmd: b}
	if len(cmd.Args()) != 2 {
		return cmd, redis.ErrInvalidArgNum
	}
	cmd.Key = string(cmd.Args()[0])
	cmd.Field = string(cmd.Args()[1])
	return cmd, nil
}

func (cmd *HGet) Run(w redis.Writer, red redis.Redka) (any, error) {
	val, err := red.Hash().Get(cmd.Key, cmd.Field)
	if err == core.ErrNotFound {
		w.WriteNull()
		return val, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteBulk(val)
	return val, nil
}
