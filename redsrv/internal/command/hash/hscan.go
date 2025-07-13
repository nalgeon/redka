package hash

import (
	"github.com/nalgeon/redka/redsrv/internal/parser"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

// Iterates over fields and values of a hash.
// HSCAN key cursor [MATCH pattern] [COUNT count]
// https://redis.io/commands/hscan
type HScan struct {
	redis.BaseCmd
	key    string
	cursor int
	match  string
	count  int
}

func ParseHScan(b redis.BaseCmd) (HScan, error) {
	cmd := HScan{BaseCmd: b}

	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.cursor),
		parser.Named("match", parser.String(&cmd.match)),
		parser.Named("count", parser.Int(&cmd.count)),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return HScan{}, err
	}

	// all keys by default
	if cmd.match == "" {
		cmd.match = "*"
	}

	return cmd, nil
}

func (cmd HScan) Run(w redis.Writer, red redis.Redka) (any, error) {
	res, err := red.Hash().Scan(cmd.key, cmd.cursor, cmd.match, cmd.count)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	w.WriteArray(2)
	w.WriteInt(res.Cursor)
	w.WriteArray(len(res.Items) * 2)
	for _, it := range res.Items {
		w.WriteBulkString(it.Field)
		w.WriteBulk(it.Value)
	}
	return res, nil
}
