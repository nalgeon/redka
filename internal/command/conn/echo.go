package conn

import (
	"strings"

	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Echo returns the given string.
// ECHO message
// https://redis.io/commands/echo
type Echo struct {
	redis.BaseCmd
	parts []string
}

func ParseEcho(b redis.BaseCmd) (*Echo, error) {
	cmd := &Echo{BaseCmd: b}
	err := parser.New(
		parser.Strings(&cmd.parts),
	).Required(1).Run(cmd.Args())
	if err != nil {
		return cmd, err
	}
	return cmd, nil
}

func (c *Echo) Run(w redis.Writer, _ redis.Redka) (any, error) {
	out := strings.Join(c.parts, " ")
	w.WriteAny(out)
	return out, nil
}
