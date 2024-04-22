package command

import (
	"github.com/nalgeon/redka/internal/parser"
)

// Deletes one or more keys.
// DEL key [key ...]
// https://redis.io/commands/del
type Del struct {
	baseCmd
	keys []string
}

func parseDel(b baseCmd) (*Del, error) {
	cmd := &Del{baseCmd: b}
	err := parser.New(
		parser.Strings(&cmd.keys),
	).Required(1).Run(cmd.args)
	if err != nil {
		return cmd, err
	}
	return cmd, nil
}

func (cmd *Del) Run(w Writer, red Redka) (any, error) {
	count, err := red.Key().Delete(cmd.keys...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
