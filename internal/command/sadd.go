package command

// SAdd adds one or more elements to set.
// SADD key elem [elem...]
// For more information: https://redis.io/docs/latest/commands/sadd/
type SAdd struct {
	baseCmd
	key   string
	elems []any
}

// parseSAdd parses SAdd command and creates *SAdd object.
func parseSAdd(b baseCmd) (*SAdd, error) {
	cmd := &SAdd{baseCmd: b}
	if len(cmd.args) < 2 {
		return nil, ErrInvalidArgNum
	}
	cmd.key = string(cmd.args[0])
	for _, arg := range cmd.args[1:] {
		cmd.elems = append(cmd.elems, arg)
	}
	return cmd, nil
}

func (cmd *SAdd) Run(w Writer, red Redka) (any, error) {
	n, err := red.Set().Add(cmd.key, cmd.elems...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
