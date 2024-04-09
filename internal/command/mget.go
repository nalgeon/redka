package command

// Atomically returns the string values of one or more keys.
// MGET key [key ...]
// https://redis.io/commands/mget
type MGet struct {
	baseCmd
	keys []string
}

func parseMGet(b baseCmd) (*MGet, error) {
	cmd := &MGet{baseCmd: b}
	if len(cmd.args) < 1 {
		return cmd, ErrInvalidArgNum(cmd.name)
	}
	cmd.keys = make([]string, len(cmd.args))
	for i, arg := range cmd.args {
		cmd.keys[i] = string(arg)
	}
	return cmd, nil
}

func (cmd *MGet) Run(w Writer, red Redka) (any, error) {
	vals, err := red.Str().GetMany(cmd.keys...)
	if err != nil {
		w.WriteError(err.Error())
		return nil, err
	}

	w.WriteArray(len(vals))
	for _, v := range vals {
		if v.IsEmpty() {
			w.WriteNull()
		} else {
			w.WriteBulk(v.Bytes())
		}
	}
	return vals, nil
}
