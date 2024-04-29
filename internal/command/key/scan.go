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
	Cursor int
	Match  string
	Count  int
}

func ParseScan(b redis.BaseCmd) (*Scan, error) {
	cmd := &Scan{BaseCmd: b}

	err := parser.New(
		parser.Int(&cmd.Cursor),
		parser.Named("match", parser.String(&cmd.Match)),
		parser.Named("count", parser.Int(&cmd.Count)),
	).Required(1).Run(cmd.Args())
	if err != nil {
		return cmd, err
	}

	// all keys by default
	if cmd.Match == "" {
		cmd.Match = "*"
	}

	return cmd, nil
}

func (cmd *Scan) Run(w redis.Writer, red redis.Redka) (any, error) {
	res, err := red.Key().Scan(cmd.Cursor, cmd.Match, cmd.Count)
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
