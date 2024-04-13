package command

// Renames a key only when the target key name doesn't exist.
// RENAMENX key newkey
// https://redis.io/commands/renamenx
type RenameNX struct {
	baseCmd
	key    string
	newKey string
}

func parseRenameNX(b baseCmd) (*RenameNX, error) {
	cmd := &RenameNX{baseCmd: b}
	if len(cmd.args) != 2 {
		return cmd, ErrInvalidArgNum
	}
	cmd.key = string(cmd.args[0])
	cmd.newKey = string(cmd.args[1])
	return cmd, nil
}

func (cmd *RenameNX) Run(w Writer, red Redka) (any, error) {
	ok, err := red.Key().RenameNX(cmd.key, cmd.newKey)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return false, err
	}
	if ok {
		w.WriteInt(1)
	} else {
		w.WriteInt(0)
	}
	return ok, nil
}
