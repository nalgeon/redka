package hash

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/parser"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

// Returns the values of multiple fields in a hash.
// HMGET key field [field ...]
// https://redis.io/commands/hmget
type HMGet struct {
	redis.BaseCmd
	key    string
	fields []string
}

func ParseHMGet(b redis.BaseCmd) (HMGet, error) {
	cmd := HMGet{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Strings(&cmd.fields),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return HMGet{}, err
	}
	return cmd, nil
}

func (cmd HMGet) Run(w redis.Writer, red redis.Redka) (any, error) {
	// Get the field-value map for requested fields.
	items, err := red.Hash().GetMany(cmd.key, cmd.fields...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	// Write the result.
	// It will contain all values in the order of fields.
	// Missing fields will have nil values.
	w.WriteArray(len(cmd.fields))
	vals := make([]core.Value, len(cmd.fields))
	for i, field := range cmd.fields {
		v, ok := items[field]
		vals[i] = v
		if ok {
			w.WriteBulk(v.Bytes())
		} else {
			w.WriteNull()
		}
	}

	return vals, nil
}
