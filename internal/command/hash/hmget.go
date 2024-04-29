package hash

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Returns the values of multiple fields in a hash.
// HMGET key field [field ...]
// https://redis.io/commands/hmget
type HMGet struct {
	redis.BaseCmd
	Key    string
	Fields []string
}

func ParseHMGet(b redis.BaseCmd) (*HMGet, error) {
	cmd := &HMGet{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.Key),
		parser.Strings(&cmd.Fields),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *HMGet) Run(w redis.Writer, red redis.Redka) (any, error) {
	// Get the field-value map for requested fields.
	items, err := red.Hash().GetMany(cmd.Key, cmd.Fields...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	// Build the result slice.
	// It will contain all values in the order of fields.
	// Missing fields will have nil values.
	vals := make([]core.Value, len(cmd.Fields))
	for i, field := range cmd.Fields {
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
