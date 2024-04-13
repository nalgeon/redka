package command

import "github.com/nalgeon/redka"

// Returns the previous string value of a key after setting it to a new value.
// GETSET key value
// https://redis.io/commands/getset
type GetSet struct {
	baseCmd
	key   string
	value []byte
}

func parseGetSet(b baseCmd) (*GetSet, error) {
	cmd := &GetSet{baseCmd: b}
	if len(cmd.args) != 2 {
		return cmd, ErrInvalidArgNum
	}
	cmd.key = string(cmd.args[0])
	cmd.value = cmd.args[1]
	return cmd, nil
}

func (cmd *GetSet) Run(w Writer, red *redka.Tx) (any, error) {
	val, err := red.Str().GetSet(cmd.key, cmd.value, 0)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if !val.Exists() {
		w.WriteNull()
		return val, nil
	}
	w.WriteBulk(val)
	return val, nil
}
