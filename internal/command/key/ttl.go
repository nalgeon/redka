package key

import (
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

// Returns the expiration time in seconds of a key.
// TTL key
// https://redis.io/commands/ttl
type TTL struct {
	redis.BaseCmd
	key string
}

func ParseTTL(b redis.BaseCmd) (TTL, error) {
	cmd := TTL{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return TTL{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd TTL) Run(w redis.Writer, red redis.Redka) (any, error) {
	k, err := red.Key().Get(cmd.key)
	if err == core.ErrNotFound {
		w.WriteInt(-2)
		return -2, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if k.ETime == nil {
		w.WriteInt(-1)
		return -1, nil
	}
	ttl := int(*k.ETime/1000 - time.Now().Unix())
	w.WriteInt(ttl)
	return ttl, nil
}
