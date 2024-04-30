package hash

import "github.com/nalgeon/redka/internal/redis"

// Determines whether a field exists in a hash.
// HEXISTS key field
// https://redis.io/commands/hexists
type HExists struct {
	redis.BaseCmd
	key   string
	field string
}

func ParseHExists(b redis.BaseCmd) (*HExists, error) {
	cmd := &HExists{BaseCmd: b}
	if len(cmd.Args()) != 2 {
		return cmd, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	cmd.field = string(cmd.Args()[1])
	return cmd, nil
}

func (cmd *HExists) Run(w redis.Writer, red redis.Redka) (any, error) {
	ok, err := red.Hash().Exists(cmd.key, cmd.field)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if ok {
		w.WriteInt(1)
	} else {
		w.WriteInt(0)
	}
	return ok, nil
}
