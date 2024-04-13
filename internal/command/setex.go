package command

import (
	"strconv"
	"time"

	"github.com/nalgeon/redka"
)

// Sets the string value and expiration time of a key.
// Creates the key if it doesn't exist.
// SETEX key seconds value
// https://redis.io/commands/setex
type SetEX struct {
	baseCmd
	key   string
	value []byte
	ttl   time.Duration
}

func parseSetEX(b baseCmd, multi int) (*SetEX, error) {
	cmd := &SetEX{baseCmd: b}
	if len(cmd.args) != 3 {
		return cmd, ErrInvalidArgNum
	}

	cmd.key = string(cmd.args[0])
	cmd.value = cmd.args[2]

	ttl, err := strconv.Atoi(string(cmd.args[1]))
	if err != nil {
		return cmd, ErrInvalidInt
	}
	cmd.ttl = time.Duration(multi*ttl) * time.Millisecond

	return cmd, nil
}

func (cmd *SetEX) Run(w Writer, red *redka.Tx) (any, error) {
	err := red.Str().SetExpires(cmd.key, cmd.value, cmd.ttl)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return true, nil
}
