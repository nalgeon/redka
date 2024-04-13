package command

import "github.com/nalgeon/redka"

// Remove all keys from the current database.
// FLUSHDB
// https://redis.io/commands/flushdb
type FlushDB struct {
	baseCmd
}

func parseFlushDB(b baseCmd) (*FlushDB, error) {
	cmd := &FlushDB{baseCmd: b}
	if len(cmd.args) != 0 {
		return cmd, ErrSyntaxError
	}
	return cmd, nil
}

func (cmd *FlushDB) Run(w Writer, red *redka.Tx) (any, error) {
	err := red.Key().DeleteAll()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return false, err
	}
	w.WriteString("OK")
	return true, nil
}
