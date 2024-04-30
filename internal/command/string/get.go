package string

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

// Get returns the string value of a key.
// GET key
// https://redis.io/commands/get
type Get struct {
	redis.BaseCmd
	key string
}

func ParseGet(b redis.BaseCmd) (*Get, error) {
	cmd := &Get{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return cmd, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd *Get) Run(w redis.Writer, red redis.Redka) (any, error) {
	val, err := red.Str().Get(cmd.key)
	if err == core.ErrNotFound {
		w.WriteNull()
		return val, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteBulk(val)
	return val, nil
}
