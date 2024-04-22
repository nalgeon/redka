package command

import "github.com/nalgeon/redka/internal/parser"

// Iterates over fields and values of a hash.
// HSCAN key cursor [MATCH pattern] [COUNT count]
// https://redis.io/commands/hscan
type HScan struct {
	baseCmd
	key    string
	cursor int
	match  string
	count  int
}

func parseHScan(b baseCmd) (*HScan, error) {
	cmd := &HScan{baseCmd: b}

	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.cursor),
		parser.NamedString("match", &cmd.match),
		parser.NamedInt("count", &cmd.count),
	).Required(2).Run(cmd.args)
	if err != nil {
		return cmd, err
	}

	// all keys by default
	if cmd.match == "" {
		cmd.match = "*"
	}

	return cmd, nil
}

func (cmd *HScan) Run(w Writer, red Redka) (any, error) {
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
