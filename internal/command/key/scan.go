package key

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Iterates over the key names in the database.
// SCAN cursor [MATCH pattern] [COUNT count]
// https://redis.io/commands/scan
type Scan struct {
	redis.BaseCmd
	cursor int
	match  string
	count  int
}

func ParseScan(b redis.BaseCmd) (*Scan, error) {
	cmd := &Scan{BaseCmd: b}

	err := parser.New(
		parser.Int(&cmd.cursor),
		parser.Named("match", parser.String(&cmd.match)),
		parser.Named("count", parser.Int(&cmd.count)),
	).Required(1).Run(cmd.Args())
	if err != nil {
		return cmd, err
	}

	// all keys by default
	if cmd.match == "" {
		cmd.match = "*"
	}

	return cmd, nil
}

func (cmd *Scan) Run(w redis.Writer, red redis.Redka) (any, error) {
	res, err := red.Key().Scan(cmd.cursor, cmd.match, cmd.count)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	w.WriteArray(2)
	w.WriteInt(res.Cursor)
	w.WriteArray(len(res.Keys))
	for _, k := range res.Keys {
		w.WriteBulkString(k.Key)
	}
	return res, nil
}
