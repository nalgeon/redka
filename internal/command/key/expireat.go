package key

import (
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Sets the expiration time of a key to a Unix timestamp.
// EXPIREAT key unix-time-seconds
// https://redis.io/commands/expireat
type ExpireAt struct {
	redis.BaseCmd
	key string
	at  time.Time
}

func ParseExpireAt(b redis.BaseCmd, multi int) (*ExpireAt, error) {
	cmd := &ExpireAt{BaseCmd: b}

	var at int
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&at),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return cmd, err
	}

	cmd.at = time.UnixMilli(int64(multi * at))
	return cmd, nil
}

func (cmd *ExpireAt) Run(w redis.Writer, red redis.Redka) (any, error) {
	err := red.Key().ExpireAt(cmd.key, cmd.at)
	if err != nil && err != core.ErrNotFound {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if err == core.ErrNotFound {
		w.WriteInt(0)
		return false, nil
	}
	w.WriteInt(1)
	return true, nil
}
