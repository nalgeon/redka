package command

import (
	"strings"

	"github.com/nalgeon/redka/internal/parser"
)

const (
	PONG = "PONG"
)

// Returns PONG if no argument is provided, otherwise return a copy of the argument as a bulk
// https://redis.io/commands/ping
type Ping struct {
	baseCmd
	parts []string
}

func parsePing(b baseCmd) (*Ping, error) {
	cmd := &Ping{baseCmd: b}
	err := parser.New(
		parser.Strings(&cmd.parts),
	).Run(cmd.args)
	if err != nil {
		return cmd, err
	}
	return cmd, nil
}

func (c *Ping) Run(w Writer, _ Redka) (any, error) {
	if len(c.parts) == 0 {
		w.WriteAny(PONG)
		return PONG, nil
	}
	out := strings.Join(c.parts, " ")
	w.WriteAny(out)
	return out, nil
}
