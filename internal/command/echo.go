package command

import (
	"strings"

	"github.com/nalgeon/redka/internal/parser"
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
	err := parser.New(
		parser.Strings(&cmd.parts),
	).Required(1).Run(cmd.args)
	if err != nil {
		return cmd, err
	}
	return cmd, nil
}

func (c *Echo) Run(w Writer, _ Redka) (any, error) {
	out := strings.Join(c.parts, " ")
	w.WriteAny(out)
	return out, nil
}
