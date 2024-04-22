package command

// Returns the number of members in a sorted set.
// ZCARD key
// https://redis.io/commands/zcard
type ZCard struct {
	baseCmd
	key string
}

func parseZCard(b baseCmd) (*ZCard, error) {
	cmd := &ZCard{baseCmd: b}
	if len(cmd.args) != 1 {
		return cmd, ErrInvalidArgNum
	}
	cmd.key = string(cmd.args[0])
	return cmd, nil
}

func (cmd *ZCard) Run(w Writer, red Redka) (any, error) {
	n, err := red.ZSet().Len(cmd.key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return 0, err
	}
	w.WriteInt(n)
	return n, nil
}
