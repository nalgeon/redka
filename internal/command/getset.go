package command

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

func (cmd *GetSet) Run(w Writer, red Redka) (any, error) {
	out, err := red.Str().SetWith(cmd.key, cmd.value).Run()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if !out.Prev.Exists() {
		w.WriteNull()
		return out.Prev, nil
	}
	w.WriteBulk(out.Prev)
	return out.Prev, nil
}
