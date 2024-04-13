package command

import (
	"strings"
)

// Echo returns the given string.
// ECHO message
// https://redis.io/commands/echo
type Echo struct {
	baseCmd
	parts []string
}

func parseEcho(b baseCmd) (*Echo, error) {
	cmd := &Echo{baseCmd: b}
	if len(b.args) < 1 {
		return cmd, ErrInvalidArgNum
	}
	cmd.parts = make([]string, len(b.args))
	for i := 0; i < len(b.args); i++ {
		cmd.parts[i] = string(cmd.args[i])
	}
	return cmd, nil
}

func (c *Echo) Run(w Writer, _ Redka) (any, error) {
	out := strings.Join(c.parts, " ")
	w.WriteAny(out)
	return out, nil
}
