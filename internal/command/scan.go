package command

import (
	"strconv"
)

// Iterates over the key names in the database.
// SCAN cursor [MATCH pattern] [COUNT count]
// https://redis.io/commands/keys
type Scan struct {
	baseCmd
	cursor int
	match  string
	count  int
}

func parseScan(b baseCmd) (*Scan, error) {
	parseMatch := func(cmd *Scan, idx int) error {
		if len(cmd.args) < idx+1 {
			return ErrSyntaxError
		}
		cmd.match = string(cmd.args[idx])
		return nil
	}

	parseCount := func(cmd *Scan, idx int) error {
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

	cmd := &Scan{baseCmd: b}
	if len(cmd.args) < 1 || len(cmd.args) > 5 {
		return cmd, ErrInvalidArgNum(cmd.name)
	}
	var err error
	cmd.cursor, err = strconv.Atoi(string(cmd.args[0]))
	if err != nil {
		return cmd, ErrInvalidCursor
	}

	if len(cmd.args) > 1 {
		switch string(cmd.args[1]) {
		case "match":
			err = parseMatch(cmd, 2)
		case "count":
			err = parseCount(cmd, 2)
		default:
			err = ErrSyntaxError
		}
		if err != nil {
			return cmd, err
		}
	}

	if len(cmd.args) > 3 {
		switch string(cmd.args[3]) {
		case "match":
			err = parseMatch(cmd, 4)
		case "count":
			err = parseCount(cmd, 4)
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

func (cmd *Scan) Run(w Writer, red Redka) (any, error) {
	res, err := red.Key().Scan(cmd.cursor, cmd.match, cmd.count)
	if err != nil {
		w.WriteError(err.Error())
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
