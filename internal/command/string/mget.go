package string

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

// Atomically returns the string values of one or more keys.
// MGET key [key ...]
// https://redis.io/commands/mget
type MGet struct {
	redis.BaseCmd
	keys []string
}

func ParseMGet(b redis.BaseCmd) (*MGet, error) {
	cmd := &MGet{BaseCmd: b}
	if len(cmd.Args()) < 1 {
		return cmd, redis.ErrInvalidArgNum
	}
	cmd.keys = make([]string, len(cmd.Args()))
	for i, arg := range cmd.Args() {
		cmd.keys[i] = string(arg)
	}
	return cmd, nil
}

func (cmd *MGet) Run(w redis.Writer, red redis.Redka) (any, error) {
	// Get the key-value map for requested keys.
	items, err := red.Str().GetMany(cmd.keys...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	// Write the result.
	// It will contain all values in the order of keys.
	// Missing keys will have nil values.
	w.WriteArray(len(cmd.keys))
	vals := make([]core.Value, len(cmd.keys))
	for i, key := range cmd.keys {
		v, ok := items[key]
		vals[i] = v
		if ok {
			w.WriteBulk(v.Bytes())
		} else {
			w.WriteNull()
		}
	}

	return vals, nil
}
