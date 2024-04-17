package command

// Atomically creates or modifies the string values of one or more keys.
// MSET key value [key value ...]
// https://redis.io/commands/mset
type MSet struct {
	baseCmd
	items map[string]any
}

func parseMSet(b baseCmd) (*MSet, error) {
	cmd := &MSet{baseCmd: b}
	if len(cmd.args) < 2 || len(cmd.args)%2 != 0 {
		return cmd, ErrInvalidArgNum
	}

	cmd.items = make(map[string]any, len(cmd.args)/2)
	for i := 0; i < len(cmd.args); i += 2 {
		cmd.items[string(cmd.args[i])] = cmd.args[i+1]
	}

	return cmd, nil
}

func (cmd *MSet) Run(w Writer, red Redka) (any, error) {
	err := red.Str().SetMany(cmd.items)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return true, nil
}
