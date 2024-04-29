package string

import "github.com/nalgeon/redka/internal/redis"

// Increments the integer value of a key by one.
// Uses 0 as initial value if the key doesn't exist.
// INCR key
// https://redis.io/commands/incr
//
// Decrements the integer value of a key by one.
// Uses 0 as initial value if the key doesn't exist.
// DECR key
// https://redis.io/commands/decr
type Incr struct {
	redis.BaseCmd
	Key   string
	Delta int
}

func ParseIncr(b redis.BaseCmd, sign int) (*Incr, error) {
	cmd := &Incr{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return cmd, redis.ErrInvalidArgNum
	}
	cmd.Key = string(cmd.Args()[0])
	cmd.Delta = sign
	return cmd, nil
}

func (cmd *Incr) Run(w redis.Writer, red redis.Redka) (any, error) {
	val, err := red.Str().Incr(cmd.Key, cmd.Delta)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(val)
	return val, nil
}
