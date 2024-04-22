package command

import (
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/parser"
)

// Sets the expiration time of a key in seconds.
// EXPIRE key seconds
// https://redis.io/commands/expire
type Expire struct {
	baseCmd
	key string
	ttl time.Duration
}

func parseExpire(b baseCmd, multi int) (*Expire, error) {
	cmd := &Expire{baseCmd: b}

	var ttl int
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&ttl),
	).Required(2).Run(cmd.args)
	if err != nil {
		return cmd, err
	}

	cmd.ttl = time.Duration(multi*ttl) * time.Millisecond
	return cmd, nil
}

func (cmd *Expire) Run(w Writer, red Redka) (any, error) {
	err := red.Key().Expire(cmd.key, cmd.ttl)
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
