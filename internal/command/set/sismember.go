package set

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Determines whether a member belongs to a set.
// SISMEMBER key member
// https://redis.io/commands/sismember
type SIsMember struct {
	redis.BaseCmd
	key    string
	member []byte
}

func ParseSIsMember(b redis.BaseCmd) (SIsMember, error) {
	cmd := SIsMember{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Bytes(&cmd.member),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return SIsMember{}, err
	}
	return cmd, nil
}

func (cmd SIsMember) Run(w redis.Writer, red redis.Redka) (any, error) {
	ok, err := red.Set().Exists(cmd.key, cmd.member)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if ok {
		w.WriteInt(1)
	} else {
		w.WriteInt(0)
	}
	return ok, nil
}
