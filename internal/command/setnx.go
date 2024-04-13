package command

import "github.com/nalgeon/redka"

// Set the string value of a key only when the key doesn't exist.
// SETNX key value
// https://redis.io/commands/setnx
type SetNX struct {
	baseCmd
	key   string
	value []byte
}

func parseSetNX(b baseCmd) (*SetNX, error) {
	cmd := &SetNX{baseCmd: b}
	if len(cmd.args) != 2 {
		return cmd, ErrInvalidArgNum
	}
	cmd.key = string(cmd.args[0])
	cmd.value = cmd.args[1]
	return cmd, nil
}

func (cmd *SetNX) Run(w Writer, red *redka.Tx) (any, error) {
	ok, err := red.Str().SetNotExists(cmd.key, cmd.value, 0)
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
