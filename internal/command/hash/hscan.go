package hash

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Iterates over fields and values of a hash.
// HSCAN key cursor [MATCH pattern] [COUNT count]
// https://redis.io/commands/hscan
type HScan struct {
	redis.BaseCmd
	Key    string
	Cursor int
	Match  string
	Count  int
}

func ParseHScan(b redis.BaseCmd) (*HScan, error) {
	cmd := &HScan{BaseCmd: b}

	err := parser.New(
		parser.String(&cmd.Key),
		parser.Int(&cmd.Cursor),
		parser.Named("match", parser.String(&cmd.Match)),
		parser.Named("count", parser.Int(&cmd.Count)),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return cmd, err
	}

	// all keys by default
	if cmd.Match == "" {
		cmd.Match = "*"
	}

	return cmd, nil
}

func (cmd *HScan) Run(w redis.Writer, red redis.Redka) (any, error) {
	res, err := red.Hash().Scan(cmd.Key, cmd.Cursor, cmd.Match, cmd.Count)
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
