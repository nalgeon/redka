package command

// Returns all key names that match a pattern.
// KEYS pattern
// https://redis.io/commands/keys
type Keys struct {
	baseCmd
	pattern string
}

func parseKeys(b baseCmd) (*Keys, error) {
	cmd := &Keys{baseCmd: b}
	if len(cmd.args) != 1 {
		return cmd, ErrInvalidArgNum(cmd.name)
	}
	cmd.pattern = string(cmd.args[0])
	return cmd, nil
}

func (cmd *Keys) Run(w Writer, red Redka) (any, error) {
	keys, err := red.Key().Search(cmd.pattern)

	if err != nil {
		w.WriteError(err.Error())
		return nil, err
	}
	w.WriteArray(len(keys))
	for _, key := range keys {
		w.WriteBulkString(key.Key)
	}
	return keys, nil
}
