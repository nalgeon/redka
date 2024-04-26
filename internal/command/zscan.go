package command

import "github.com/nalgeon/redka/internal/parser"

// Iterates over members and scores of a sorted set.
// ZSCAN key cursor [MATCH pattern] [COUNT count]
// https://redis.io/commands/zscan
type ZScan struct {
	baseCmd
	key    string
	cursor int
	match  string
	count  int
}

func parseZScan(b baseCmd) (*ZScan, error) {
	cmd := &ZScan{baseCmd: b}

	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.cursor),
		parser.Named("match", parser.String(&cmd.match)),
		parser.Named("count", parser.Int(&cmd.count)),
	).Required(2).Run(cmd.args)
	if err != nil {
		return cmd, err
	}

	// all elements by default
	if cmd.match == "" {
		cmd.match = "*"
	}

	return cmd, nil
}

func (cmd *ZScan) Run(w Writer, red Redka) (any, error) {
	res, err := red.ZSet().Scan(cmd.key, cmd.cursor, cmd.match, cmd.count)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	w.WriteArray(2)
	w.WriteInt(res.Cursor)
	w.WriteArray(len(res.Items) * 2)
	for _, it := range res.Items {
		w.WriteBulk(it.Elem)
		writeFloat(w, it.Score)
	}
	return res, nil
}
