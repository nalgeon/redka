package command

import (
	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/core"
)

// Returns the values of multiple fields in a hash.
// HMGET key field [field ...]
// https://redis.io/commands/hmget
type HMGet struct {
	baseCmd
	key    string
	fields []string
}

func parseHMGet(b baseCmd) (*HMGet, error) {
	cmd := &HMGet{baseCmd: b}
	if len(cmd.args) < 2 {
		return cmd, ErrInvalidArgNum
	}
	cmd.key = string(cmd.args[0])
	cmd.fields = make([]string, len(cmd.args)-1)
	for i, arg := range cmd.args[1:] {
		cmd.fields[i] = string(arg)
	}
	return cmd, nil
}

func (cmd *HMGet) Run(w Writer, red *redka.Tx) (any, error) {
	// Get the field-value map for requested fields.
	items, err := red.Hash().GetMany(cmd.key, cmd.fields...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	// Build the result slice.
	// It will contain all values in the order of fields.
	// Missing fields will have nil values.
	vals := make([]core.Value, len(cmd.fields))
	for i, field := range cmd.fields {
		vals[i] = items[field]
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
