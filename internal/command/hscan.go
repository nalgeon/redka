package command

import (
	"strconv"
)

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
	if len(cmd.args) < 2 {
		return cmd, ErrInvalidArgNum
	}

	var err error
	cmd.key = string(cmd.args[0])
	cmd.cursor, err = strconv.Atoi(string(cmd.args[1]))
	if err != nil {
		return cmd, ErrInvalidCursor
	}
	cmd.args = cmd.args[2:]

	err = cmd.parseMatch()
	if err != nil {
		return cmd, err
	}

	err = cmd.parseCount()
	if err != nil {
		return cmd, err
	}

	if len(cmd.args) > 0 {
		return cmd, ErrSyntaxError
	}

	// all keys by default
	if cmd.match == "" {
		cmd.match = "*"
	}

	return cmd, nil
}

func (cmd *HScan) parseMatch() error {
	if len(cmd.args) == 0 {
		return nil
	}
	if string(cmd.args[0]) != "match" {
		return nil
	}
	cmd.match = string(cmd.args[1])
	cmd.args = cmd.args[2:]
	return nil
}

func (cmd *HScan) parseCount() error {
	if len(cmd.args) == 0 {
		return nil
	}
	if string(cmd.args[0]) != "count" {
		return nil
	}

	var err error
	cmd.count, err = strconv.Atoi(string(cmd.args[1]))
	if err != nil {
		return ErrInvalidInt
	}

	cmd.args = cmd.args[2:]
	return nil
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
