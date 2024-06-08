package set

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Iterates over members of a set.
// SSCAN key cursor [MATCH pattern] [COUNT count]
// https://redis.io/commands/sscan
type SScan struct {
	redis.BaseCmd
	key    string
	cursor int
	match  string
	count  int
}

func ParseSScan(b redis.BaseCmd) (SScan, error) {
	cmd := SScan{BaseCmd: b}

	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.cursor),
		parser.Named("match", parser.String(&cmd.match)),
		parser.Named("count", parser.Int(&cmd.count)),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return SScan{}, err
	}

	// all elements by default
	if cmd.match == "" {
		cmd.match = "*"
	}

	return cmd, nil
}

func (cmd SScan) Run(w redis.Writer, red redis.Redka) (any, error) {
	res, err := red.Set().Scan(cmd.key, cmd.cursor, cmd.match, cmd.count)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	w.WriteArray(2)
	w.WriteInt(res.Cursor)
	w.WriteArray(len(res.Items))
	for _, val := range res.Items {
		w.WriteBulk(val)
	}
	return res, nil
}
