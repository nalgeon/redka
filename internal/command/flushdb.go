package command

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

func (cmd *FlushDB) Run(w Writer, red Redka) (any, error) {
	err := red.Flush()
	if err != nil {
		w.WriteError(err.Error())
		return false, err
	}
	w.WriteString("OK")
	return true, nil
}
