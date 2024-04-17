package command

// Sets the value of a field in a hash only when the field doesn't exist.
// HSETNX key field value
// https://redis.io/commands/hsetnx
type HSetNX struct {
	baseCmd
	key   string
	field string
	value []byte
}

func parseHSetNX(b baseCmd) (*HSetNX, error) {
	cmd := &HSetNX{baseCmd: b}
	if len(cmd.args) != 3 {
		return cmd, ErrInvalidArgNum
	}
	cmd.key = string(cmd.args[0])
	cmd.field = string(cmd.args[1])
	cmd.value = cmd.args[2]
	return cmd, nil
}

func (cmd *HSetNX) Run(w Writer, red Redka) (any, error) {
	ok, err := red.Hash().SetNotExists(cmd.key, cmd.field, cmd.value)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if ok {
		w.WriteInt(1)
	} else {
		w.WriteInt(0)
	}
	return ok, nil
}
