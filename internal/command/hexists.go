package command

// Determines whether a field exists in a hash.
// HEXISTS key field
// https://redis.io/commands/hexists
type HExists struct {
	baseCmd
	key   string
	field string
}

func parseHExists(b baseCmd) (*HExists, error) {
	cmd := &HExists{baseCmd: b}
	if len(cmd.args) != 2 {
		return cmd, ErrInvalidArgNum(cmd.name)
	}
	cmd.key = string(cmd.args[0])
	cmd.field = string(cmd.args[1])
	return cmd, nil
}

func (cmd *HExists) Run(w Writer, red Redka) (any, error) {
	ok, err := red.Hash().Exists(cmd.key, cmd.field)
	if err != nil {
		w.WriteError(translateError(err))
		return nil, err
	}
	if ok {
		w.WriteInt(1)
	} else {
		w.WriteInt(0)
	}
	return ok, nil
}
