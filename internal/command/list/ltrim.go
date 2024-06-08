package list

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Removes elements from both ends a list.
// LTRIM key start stop
// https://redis.io/commands/ltrim
type LTrim struct {
	redis.BaseCmd
	key   string
	start int
	stop  int
}

func ParseLTrim(b redis.BaseCmd) (LTrim, error) {
	cmd := LTrim{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.start),
		parser.Int(&cmd.stop),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return LTrim{}, err
	}
	return cmd, nil
}

func (cmd LTrim) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.List().Trim(cmd.key, cmd.start, cmd.stop)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return n, nil
}
