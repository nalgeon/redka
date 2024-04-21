package command

import "github.com/nalgeon/redka/internal/core"

// Returns the value of a field in a hash.
// HGET key field
// https://redis.io/commands/hget
type HGet struct {
	baseCmd
	key   string
	field string
}

func parseHGet(b baseCmd) (*HGet, error) {
	cmd := &HGet{baseCmd: b}
	if len(cmd.args) != 2 {
		return cmd, ErrInvalidArgNum
	}
	cmd.key = string(cmd.args[0])
	cmd.field = string(cmd.args[1])
	return cmd, nil
}

func (cmd *HGet) Run(w Writer, red Redka) (any, error) {
	val, err := red.Hash().Get(cmd.key, cmd.field)
	if err == core.ErrNotFound {
		w.WriteNull()
		return val, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return val, err
	}
	w.WriteBulk(val)
	return val, nil
}
