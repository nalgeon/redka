package command

// Returns all fields in a hash.
// HKEYS key
// https://redis.io/commands/hkeys
type HKeys struct {
	baseCmd
	key string
}

func parseHKeys(b baseCmd) (*HKeys, error) {
	cmd := &HKeys{baseCmd: b}
	if len(cmd.args) != 1 {
		return cmd, ErrInvalidArgNum
	}
	cmd.key = string(cmd.args[0])
	return cmd, nil
}

func (cmd *HKeys) Run(w Writer, red Redka) (any, error) {
	fields, err := red.Hash().Fields(cmd.key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteArray(len(fields))
	for _, field := range fields {
		w.WriteBulkString(field)
	}
	return fields, nil
}
