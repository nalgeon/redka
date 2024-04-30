package conn

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

const (
	PONG = "PONG"
)

// Returns the server's liveliness response.
// https://redis.io/commands/ping
type Ping struct {
	redis.BaseCmd
	message string
}

func ParsePing(b redis.BaseCmd) (*Ping, error) {
	cmd := &Ping{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.message),
	).Required(0).Run(cmd.Args())
	if err != nil {
		return cmd, err
	}
	return cmd, nil
}

func (c *Ping) Run(w redis.Writer, _ redis.Redka) (any, error) {
	if c.message == "" {
		w.WriteAny(PONG)
		return PONG, nil
	}
	w.WriteBulkString(c.message)
	return c.message, nil
}
