package command

// Appends a string to the value of a key. Creates the key if it doesn't exist.
// APPEND key value
// https://redis.io/commands/append
type Append struct {
	baseCmd
	key   string
	value string
}

func parseAppend(b baseCmd) (*Append, error) {
	cmd := &Append{baseCmd: b}
	if len(cmd.args) != 2 {
		return cmd, ErrInvalidArgNum(cmd.name)
	}
	cmd.key = string(cmd.args[0])
	cmd.value = string(cmd.args[1])
	return cmd, nil
}

func (cmd *Append) Run(w Writer, red Redka) (any, error) {
	length, err := red.Str().Append(cmd.key, cmd.value)

	if err != nil {
		w.WriteError(err.Error())
		return nil, err
	}
	w.WriteInt(length)
	return length, nil
}
