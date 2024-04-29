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
	Keys []string
}

func ParseMGet(b redis.BaseCmd) (*MGet, error) {
	cmd := &MGet{BaseCmd: b}
	if len(cmd.Args()) < 1 {
		return cmd, redis.ErrInvalidArgNum
	}
	cmd.Keys = make([]string, len(cmd.Args()))
	for i, arg := range cmd.Args() {
		cmd.Keys[i] = string(arg)
	}
	return cmd, nil
}

func (cmd *MGet) Run(w redis.Writer, red redis.Redka) (any, error) {
	// Get the key-value map for requested keys.
	items, err := red.Str().GetMany(cmd.Keys...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	// Build the result slice.
	// It will contain all values in the order of keys.
	// Missing keys will have nil values.
	vals := make([]core.Value, len(cmd.Keys))
	for i, key := range cmd.Keys {
		vals[i] = items[key]
	}

	// Write the result.
	w.WriteArray(len(vals))
	for _, v := range vals {
		if v.Exists() {
			w.WriteBulk(v.Bytes())
		} else {
			w.WriteNull()
		}
	}
	return vals, nil
}
