package command

import "github.com/nalgeon/redka/internal/parser"

// Atomically modifies the string values of one
// or more keys only when all keys don't exist.
// MSETNX key value [key value ...]
// https://redis.io/commands/msetnx
type MSetNX struct {
	baseCmd
	items map[string]any
}

func parseMSetNX(b baseCmd) (*MSetNX, error) {
	cmd := &MSetNX{baseCmd: b}
	err := parser.New(
		parser.AnyMap(&cmd.items),
	).Required(2).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *MSetNX) Run(w Writer, red Redka) (any, error) {
	ok, err := red.Str().SetManyNX(cmd.items)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if ok {
		w.WriteInt(1)
	} else {
		w.WriteInt(0)
	}
	return ok, nil
}
