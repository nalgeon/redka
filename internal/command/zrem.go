package command

import (
	"github.com/nalgeon/redka/internal/parser"
)

// Removes one or more members from a sorted set.
// ZREM key member [member ...]
// https://redis.io/commands/zrem
type ZRem struct {
	baseCmd
	key     string
	members []any
}

func parseZRem(b baseCmd) (*ZRem, error) {
	cmd := &ZRem{baseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Anys(&cmd.members),
	).Required(2).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZRem) Run(w Writer, red Redka) (any, error) {
	n, err := red.ZSet().Delete(cmd.key, cmd.members...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
