package command

import (
	"github.com/nalgeon/redka/internal/core"
)

// Atomically returns the string values of one or more keys.
// MGET key [key ...]
// https://redis.io/commands/mget
type MGet struct {
	baseCmd
	keys []string
}

func parseMGet(b baseCmd) (*MGet, error) {
	cmd := &MGet{baseCmd: b}
	if len(cmd.args) < 1 {
		return cmd, ErrInvalidArgNum
	}
	cmd.keys = make([]string, len(cmd.args))
	for i, arg := range cmd.args {
		cmd.keys[i] = string(arg)
	}
	return cmd, nil
}

func (cmd *MGet) Run(w Writer, red Redka) (any, error) {
	// Get the key-value map for requested keys.
	items, err := red.Str().GetMany(cmd.keys...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	// Build the result slice.
	// It will contain all values in the order of keys.
	// Missing keys will have nil values.
	vals := make([]core.Value, len(cmd.keys))
	for i, key := range cmd.keys {
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
