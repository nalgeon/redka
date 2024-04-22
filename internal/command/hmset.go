package command

import "github.com/nalgeon/redka/internal/parser"

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
	err := parser.New(
		parser.String(&cmd.key),
		parser.AnyMap(&cmd.items),
	).Required(3).Run(cmd.args)
	if err != nil {
		return nil, err
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
