package command

// Sets the values of multiple fields in a hash.
// HMSET key field value [field value ...]
// https://redis.io/commands/hmset
type HMSet struct {
	baseCmd
	key   string
	items map[string]any
}

func parseHMSet(b baseCmd) (*HMSet, error) {
	cmd := &HMSet{baseCmd: b}
	if len(cmd.args) < 3 || len(cmd.args)%2 != 1 {
		return cmd, ErrInvalidArgNum
	}
	cmd.key = string(cmd.args[0])
	cmd.items = make(map[string]any, (len(cmd.args)-1)/2)
	for i := 1; i < len(cmd.args); i += 2 {
		cmd.items[string(cmd.args[i])] = cmd.args[i+1]
	}
	return cmd, nil
}

func (cmd *HMSet) Run(w Writer, red Redka) (any, error) {
	count, err := red.Hash().SetMany(cmd.key, cmd.items)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return count, nil
}
