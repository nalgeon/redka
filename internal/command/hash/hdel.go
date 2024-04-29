package hash

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Deletes one or more fields and their values from a hash.
// Deletes the hash if no fields remain.
// HDEL key field [field ...]
// https://redis.io/commands/hdel
type HDel struct {
	redis.BaseCmd
	Key    string
	Fields []string
}

func ParseHDel(b redis.BaseCmd) (*HDel, error) {
	cmd := &HDel{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.Key),
		parser.Strings(&cmd.Fields),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *HDel) Run(w redis.Writer, red redis.Redka) (any, error) {
	count, err := red.Hash().Delete(cmd.Key, cmd.Fields...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
