package command

// Deletes one or more fields and their values from a hash.
// Deletes the hash if no fields remain.
// HDEL key field [field ...]
// https://redis.io/commands/hdel
type HDel struct {
	baseCmd
	key    string
	fields []string
}

func parseHDel(b baseCmd) (*HDel, error) {
	cmd := &HDel{baseCmd: b}
	if len(cmd.args) < 2 {
		return cmd, ErrInvalidArgNum(cmd.name)
	}
	cmd.key = string(cmd.args[0])
	cmd.fields = make([]string, len(cmd.args)-1)
	for i, arg := range cmd.args[1:] {
		cmd.fields[i] = string(arg)
	}
	return cmd, nil
}

func (cmd *HDel) Run(w Writer, red Redka) (any, error) {
	count, err := red.Hash().Delete(cmd.key, cmd.fields...)
	if err != nil {
		w.WriteError(translateError(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
