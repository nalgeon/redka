package set

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Moves a member from one set to another.
// SMOVE source destination member
// https://redis.io/commands/smove
type SMove struct {
	redis.BaseCmd
	src    string
	dest   string
	member []byte
}

func ParseSMove(b redis.BaseCmd) (SMove, error) {
	cmd := SMove{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.src),
		parser.String(&cmd.dest),
		parser.Bytes(&cmd.member),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return SMove{}, err
	}
	return cmd, nil
}

func (cmd SMove) Run(w redis.Writer, red redis.Redka) (any, error) {
	err := red.Set().Move(cmd.src, cmd.dest, cmd.member)
	if err == core.ErrNotFound {
		w.WriteInt(0)
		return 0, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(1)
	return 1, nil
}
