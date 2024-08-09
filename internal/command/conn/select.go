package conn

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Changes the selected database.
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
