package command

// Increments the integer value of a key by one.
// Uses 0 as initial value if the key doesn't exist.
// INCR key
// https://redis.io/commands/incr
//
// Decrements the integer value of a key by one.
// Uses 0 as initial value if the key doesn't exist.
// DECR key
// https://redis.io/commands/decr
type Incr struct {
	baseCmd
	key   string
	delta int
}

func parseIncr(b baseCmd, sign int) (*Incr, error) {
	cmd := &Incr{baseCmd: b}
	if len(cmd.args) != 1 {
		return cmd, ErrInvalidArgNum(cmd.name)
	}
	cmd.key = string(cmd.args[0])
	cmd.delta = sign
	return cmd, nil
}

func (cmd *Incr) Run(w Writer, red Redka) (any, error) {
	val, err := red.Str().Incr(cmd.key, cmd.delta)
	if err != nil {
		w.WriteError(err.Error())
		return nil, err
	}
	w.WriteInt(val)
	return val, nil
}
