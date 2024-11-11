package string

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

// Strlen returns the length of a string value.
// STRLEN key
// https://redis.io/commands/strlen
type Strlen struct {
	redis.BaseCmd
	key string
}

func ParseStrlen(b redis.BaseCmd) (Strlen, error) {
	cmd := Strlen{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return Strlen{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd Strlen) Run(w redis.Writer, red redis.Redka) (any, error) {
	val, err := red.Str().Get(cmd.key)
	if err == core.ErrNotFound {
		w.WriteInt(0)
		return 0, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(len(val))
	return len(val), nil
}
