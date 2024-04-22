package command

import "github.com/nalgeon/redka/internal/parser"

// Atomically creates or modifies the string values of one or more keys.
// MSET key value [key value ...]
// https://redis.io/commands/mset
type MSet struct {
	baseCmd
	items map[string]any
}

func parseMSet(b baseCmd) (*MSet, error) {
	cmd := &MSet{baseCmd: b}
	err := parser.New(
		parser.AnyMap(&cmd.items),
	).Required(2).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *MSet) Run(w Writer, red Redka) (any, error) {
	err := red.Str().SetMany(cmd.items)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return true, nil
}
