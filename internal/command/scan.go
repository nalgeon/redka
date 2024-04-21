package command

import (
	"strconv"
)

// Iterates over the key names in the database.
// SCAN cursor [MATCH pattern] [COUNT count]
// https://redis.io/commands/scan
type Scan struct {
	baseCmd
	cursor int
	match  string
	count  int
}

func parseScan(b baseCmd) (*Scan, error) {
	cmd := &Scan{baseCmd: b}
	if len(cmd.args) < 1 || len(cmd.args) > 5 {
		return cmd, ErrInvalidArgNum
	}

	var err error
	cmd.cursor, err = strconv.Atoi(string(cmd.args[0]))
	if err != nil {
		return cmd, ErrInvalidCursor
	}
	cmd.args = cmd.args[1:]

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

func (cmd *Scan) parseMatch() error {
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

func (cmd *Scan) parseCount() error {
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

func (cmd *Scan) Run(w Writer, red Redka) (any, error) {
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
