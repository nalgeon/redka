package hash

import "github.com/nalgeon/redka/internal/redis"

// Returns all fields in a hash.
// HKEYS key
// https://redis.io/commands/hkeys
type HKeys struct {
	redis.BaseCmd
	Key string
}

func ParseHKeys(b redis.BaseCmd) (*HKeys, error) {
	cmd := &HKeys{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return cmd, redis.ErrInvalidArgNum
	}
	cmd.Key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd *HKeys) Run(w redis.Writer, red redis.Redka) (any, error) {
	fields, err := red.Hash().Fields(cmd.Key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteArray(len(fields))
	for _, field := range fields {
		w.WriteBulkString(field)
	}
	return fields, nil
}
