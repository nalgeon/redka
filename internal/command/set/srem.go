package set

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Removes one or more members from a set.
// SREM key member [member ...]
// https://redis.io/commands/srem
type SRem struct {
	redis.BaseCmd
	key     string
	members []any
}

func ParseSRem(b redis.BaseCmd) (SRem, error) {
	cmd := SRem{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Anys(&cmd.members),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return SRem{}, err
	}
	return cmd, nil
}

func (cmd SRem) Run(w redis.Writer, red redis.Redka) (any, error) {
	count, err := red.Set().Delete(cmd.key, cmd.members...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
