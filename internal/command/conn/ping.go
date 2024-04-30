package conn

import (
	"strings"

	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

const (
	PONG = "PONG"
)

// Returns PONG if no argument is provided, otherwise return a copy of the argument as a bulk
// https://redis.io/commands/ping
type Ping struct {
	redis.BaseCmd
	Parts []string
}


func ParsePing(b redis.BaseCmd) (*Ping, error) {
	cmd := &Ping{BaseCmd: b}
	err := parser.New(
		parser.Strings(&cmd.Parts),
	).Required(1).Run(cmd.Args())
	if err != nil {
		return cmd, err
	}
	return cmd, nil
}

func (c *Ping) Run(w redis.Writer, _ redis.Redka) (any, error) {
	if len(c.Parts) == 0 {
		w.WriteAny(PONG)
		return PONG, nil
	}
	out := strings.Join(c.Parts, " ")
	w.WriteAny(out)
	return out, nil
}
