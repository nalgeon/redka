package command

import "github.com/nalgeon/redka/internal/parser"

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
	err := parser.New(
		parser.String(&cmd.key),
		parser.AnyMap(&cmd.items),
	).Required(3).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *HSet) Run(w Writer, red Redka) (any, error) {
	count, err := red.Hash().SetMany(cmd.key, cmd.items)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
