package hash

import "github.com/nalgeon/redka/internal/redis"

// Returns all values in a hash.
// HVALS key
// https://redis.io/commands/hvals
type HVals struct {
	redis.BaseCmd
	Key string
}

func ParseHVals(b redis.BaseCmd) (*HVals, error) {
	cmd := &HVals{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return cmd, redis.ErrInvalidArgNum
	}
	cmd.Key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd *HVals) Run(w redis.Writer, red redis.Redka) (any, error) {
	vals, err := red.Hash().Values(cmd.Key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteArray(len(vals))
	for _, val := range vals {
		w.WriteBulk(val)
	}
	return vals, nil
}
