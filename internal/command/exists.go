package command

// Determines whether one or more keys exist.
// EXISTS key [key ...]
// https://redis.io/commands/exists
type Exists struct {
	baseCmd
	keys []string
}

func parseExists(b baseCmd) (*Exists, error) {
	cmd := &Exists{baseCmd: b}
	if len(cmd.args) < 1 {
		return cmd, ErrInvalidArgNum(cmd.name)
	}
	cmd.keys = make([]string, len(cmd.args))
	for i, arg := range cmd.args {
		cmd.keys[i] = string(arg)
	}
	return cmd, nil
}

func (cmd *Exists) Run(w Writer, red Redka) (any, error) {
	count, err := red.Key().Count(cmd.keys...)
	if err != nil {
		w.WriteError(err.Error())
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
