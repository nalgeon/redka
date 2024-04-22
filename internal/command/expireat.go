package command

import (
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/parser"
)

// Sets the expiration time of a key to a Unix timestamp.
// EXPIREAT key unix-time-seconds
// https://redis.io/commands/expireat
type ExpireAt struct {
	baseCmd
	key string
	at  time.Time
}

func parseExpireAt(b baseCmd, multi int) (*ExpireAt, error) {
	cmd := &ExpireAt{baseCmd: b}

	var at int
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&at),
	).Required(2).Run(cmd.args)
	if err != nil {
		return cmd, err
	}

	cmd.at = time.UnixMilli(int64(multi * at))
	return cmd, nil
}

func (cmd *ExpireAt) Run(w Writer, red Redka) (any, error) {
	err := red.Key().ExpireAt(cmd.key, cmd.at)
	if err != nil && err != core.ErrNotFound {
		w.WriteError(cmd.Error(err))
		return false, err
	}
	if err == core.ErrNotFound {
		w.WriteInt(0)
		return false, nil
	}
	w.WriteInt(1)
	return true, nil
}
