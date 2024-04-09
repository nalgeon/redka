package command

// Get returns the string value of a key.
// GET key
// https://redis.io/commands/get
type Get struct {
	baseCmd
	key string
}

func parseGet(b baseCmd) (*Get, error) {
	cmd := &Get{baseCmd: b}
	if len(cmd.args) != 1 {
		return cmd, ErrInvalidArgNum(cmd.name)
	}
	cmd.key = string(cmd.args[0])
	return cmd, nil
}

func (cmd *Get) Run(w Writer, red Redka) (any, error) {
	val, err := red.Str().Get(cmd.key)
	if err != nil {
		w.WriteError(err.Error())
		return nil, err
	}
	if val.IsEmpty() {
		w.WriteNull()
		return val, nil
	}
	w.WriteBulk(val)
	return val, nil
}
