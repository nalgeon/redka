package string

import (
	"time"

	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Sets the string value and expiration time of a key.
// Creates the key if it doesn't exist.
// SETEX key seconds value
// https://redis.io/commands/setex
type SetEX struct {
	redis.BaseCmd
	Key   string
	Value []byte
	TTL   time.Duration
}

func ParseSetEX(b redis.BaseCmd, multi int) (*SetEX, error) {
	cmd := &SetEX{BaseCmd: b}
	var ttl int
	err := parser.New(
		parser.String(&cmd.Key),
		parser.Int(&ttl),
		parser.Bytes(&cmd.Value),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	cmd.TTL = time.Duration(multi*ttl) * time.Millisecond
	return cmd, nil
}

func (cmd *SetEX) Run(w redis.Writer, red redis.Redka) (any, error) {
	err := red.Str().SetExpires(cmd.Key, cmd.Value, cmd.TTL)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return true, nil
}
