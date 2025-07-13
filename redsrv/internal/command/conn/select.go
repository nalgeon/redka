package conn

import (
	"github.com/nalgeon/redka/redsrv/internal/parser"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

// Changes the selected database.
// Since Redka doesn't support multiple databases, this command is a no-op.
// SELECT index
// https://redis.io/commands/select
type Select struct {
	redis.BaseCmd
	index int
}

func ParseSelect(b redis.BaseCmd) (Select, error) {
	cmd := Select{BaseCmd: b}
	err := parser.New(
		parser.Int(&cmd.index),
	).Required(1).Run(cmd.Args())
	if err != nil {
		return Select{}, err
	}
	return cmd, nil
}

func (c Select) Run(w redis.Writer, _ redis.Redka) (any, error) {
	w.WriteString("OK")
	return true, nil
}
