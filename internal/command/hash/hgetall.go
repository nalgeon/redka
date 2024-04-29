package hash

import "github.com/nalgeon/redka/internal/redis"

// Returns all fields and values in a hash.
// HGETALL key
// https://redis.io/commands/hgetall
type HGetAll struct {
	redis.BaseCmd
	Key string
}

func ParseHGetAll(b redis.BaseCmd) (*HGetAll, error) {
	cmd := &HGetAll{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return cmd, redis.ErrInvalidArgNum
	}
	cmd.Key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd *HGetAll) Run(w redis.Writer, red redis.Redka) (any, error) {
	items, err := red.Hash().Items(cmd.Key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteArray(len(items) * 2)
	for field, val := range items {
		w.WriteBulkString(field)
		w.WriteBulk(val)
	}
	return items, nil
}
