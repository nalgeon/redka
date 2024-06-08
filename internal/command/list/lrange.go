package list

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Returns a range of elements from a list.
// LRANGE key start stop
// https://redis.io/commands/lrange
type LRange struct {
	redis.BaseCmd
	key   string
	start int
	stop  int
}

func ParseLRange(b redis.BaseCmd) (LRange, error) {
	cmd := LRange{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.start),
		parser.Int(&cmd.stop),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return LRange{}, err
	}
	return cmd, nil
}

func (cmd LRange) Run(w redis.Writer, red redis.Redka) (any, error) {
	vals, err := red.List().Range(cmd.key, cmd.start, cmd.stop)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteArray(len(vals))
	for _, v := range vals {
		w.WriteBulk(v)
	}
	return vals, nil
}
