package command

// Sets the values of one ore more fields in a hash.
// HSET key field value [field value ...]
// https://redis.io/commands/hset
type HSet struct {
	baseCmd
	key   string
	items map[string]any
}

func parseHSet(b baseCmd) (*HSet, error) {
	cmd := &HSet{baseCmd: b}
	if len(cmd.args) < 3 || len(cmd.args)%2 != 1 {
		return cmd, ErrInvalidArgNum(cmd.name)
	}
	cmd.key = string(cmd.args[0])
	cmd.items = make(map[string]any, (len(cmd.args)-1)/2)
	for i := 1; i < len(cmd.args); i += 2 {
		cmd.items[string(cmd.args[i])] = cmd.args[i+1]
	}
	return cmd, nil
}

func (cmd *HSet) Run(w Writer, red Redka) (any, error) {
	count, err := red.Hash().SetMany(cmd.key, cmd.items)
	if err != nil {
		w.WriteError(translateError(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
