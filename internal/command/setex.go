package command

import (
	"time"

	"github.com/nalgeon/redka/internal/parser"
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

	var ttl int
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&ttl),
		parser.Bytes(&cmd.value),
	).Required(3).Run(cmd.args)
	if err != nil {
		return nil, err
	}

	cmd.ttl = time.Duration(multi*ttl) * time.Millisecond

	return cmd, nil
}

func (cmd *SetEX) Run(w Writer, red Redka) (any, error) {
	err := red.Str().SetExpires(cmd.key, cmd.value, cmd.ttl)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return true, nil
}
