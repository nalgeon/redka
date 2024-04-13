package command

import (
	"strconv"

	"github.com/nalgeon/redka"
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
	parseMatch := func(cmd *HScan, idx int) error {
		if len(cmd.args) < idx+1 {
			return ErrSyntaxError
		}
		cmd.match = string(cmd.args[idx])
		return nil
	}

	parseCount := func(cmd *HScan, idx int) error {
		if len(cmd.args) < idx+1 {
			return ErrSyntaxError
		}
		var err error
		cmd.count, err = strconv.Atoi(string(cmd.args[idx]))
		if err != nil {
			return ErrInvalidInt
		}
		return nil
	}

	cmd := &HScan{baseCmd: b}
	if len(cmd.args) < 2 || len(cmd.args) > 6 {
		return cmd, ErrInvalidArgNum
	}
	var err error
	cmd.key = string(cmd.args[0])
	cmd.cursor, err = strconv.Atoi(string(cmd.args[1]))
	if err != nil {
		return cmd, ErrInvalidCursor
	}

	if len(cmd.args) > 2 {
		switch string(cmd.args[2]) {
		case "match":
			err = parseMatch(cmd, 3)
		case "count":
			err = parseCount(cmd, 3)
		default:
			err = ErrSyntaxError
		}
		if err != nil {
			return cmd, err
		}
	}

	if len(cmd.args) > 4 {
		switch string(cmd.args[4]) {
		case "match":
			err = parseMatch(cmd, 5)
		case "count":
			err = parseCount(cmd, 5)
		default:
			err = ErrSyntaxError
		}
		if err != nil {
			return cmd, err
		}
	}

	// all keys by default
	if cmd.match == "" {
		cmd.match = "*"
	}

	return cmd, nil
}

func (cmd *HScan) Run(w Writer, red *redka.Tx) (any, error) {
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
