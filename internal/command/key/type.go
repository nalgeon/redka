package key

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

// Determines the type of value stored at a key.
// TYPE key
// https://redis.io/commands/type
type Type struct {
	redis.BaseCmd
	key string
}

func ParseType(b redis.BaseCmd) (Type, error) {
	cmd := Type{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return Type{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd Type) Run(w redis.Writer, red redis.Redka) (any, error) {
	k, err := red.Key().Get(cmd.key)
	if err == core.ErrNotFound {
		w.WriteString("none")
		return "none", nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteString(k.TypeName())
	return k.TypeName(), nil
}
