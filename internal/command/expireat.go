package command

import (
	"strconv"
	"time"
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
	if len(cmd.args) != 2 {
		return cmd, ErrInvalidArgNum(cmd.name)
	}
	cmd.key = string(cmd.args[0])
	at, err := strconv.Atoi(string(cmd.args[1]))
	if err != nil {
		return cmd, err
	}
	cmd.at = time.UnixMilli(int64(multi * at))
	return cmd, nil
}

func (cmd *ExpireAt) Run(w Writer, red Redka) (any, error) {
	ok, err := red.Key().ExpireAt(cmd.key, cmd.at)
	if err != nil {
		w.WriteError(translateError(err))
		return nil, err
	}
	if ok {
		w.WriteInt(1)
	} else {
		w.WriteInt(0)
	}
	return ok, nil
}
