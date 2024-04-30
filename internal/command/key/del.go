package key

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Deletes one or more Keys.
// DEL key [key ...]
// https://redis.io/commands/del
type Del struct {
	redis.BaseCmd
	keys []string
}

func ParseDel(b redis.BaseCmd) (*Del, error) {
	cmd := &Del{BaseCmd: b}
	err := parser.New(
		parser.Strings(&cmd.keys),
	).Required(1).Run(cmd.Args())
	if err != nil {
		return cmd, err
	}
	return cmd, nil
}

func (cmd *Del) Run(w redis.Writer, red redis.Redka) (any, error) {
	count, err := red.Key().Delete(cmd.keys...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
