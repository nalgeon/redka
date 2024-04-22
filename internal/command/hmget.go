package command

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/parser"
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
	err := parser.New(
		parser.String(&cmd.key),
		parser.Strings(&cmd.fields),
	).Required(2).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *HMGet) Run(w Writer, red Redka) (any, error) {
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
