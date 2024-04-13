package command

import "github.com/nalgeon/redka"

// Deletes one or more keys.
// DEL key [key ...]
// https://redis.io/commands/del
type Del struct {
	baseCmd
	keys []string
}

func parseDel(b baseCmd) (*Del, error) {
	cmd := &Del{baseCmd: b}
	if len(cmd.args) < 1 {
		return cmd, ErrInvalidArgNum
	}
	cmd.keys = make([]string, len(cmd.args))
	for i, arg := range cmd.args {
		cmd.keys[i] = string(arg)
	}
	return cmd, nil
}

func (cmd *Del) Run(w Writer, red *redka.Tx) (any, error) {
	count, err := red.Key().Delete(cmd.keys...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
