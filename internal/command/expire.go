package command

import (
	"strconv"
	"time"
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
	if len(cmd.args) != 2 {
		return cmd, ErrInvalidArgNum
	}
	cmd.key = string(cmd.args[0])
	ttl, err := strconv.Atoi(string(cmd.args[1]))
	if err != nil {
		return cmd, ErrInvalidInt
	}
	cmd.ttl = time.Duration(multi*ttl) * time.Millisecond
	return cmd, nil
}

func (cmd *Expire) Run(w Writer, red Redka) (any, error) {
	ok, err := red.Key().Expire(cmd.key, cmd.ttl)
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
