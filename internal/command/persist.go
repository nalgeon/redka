package command

import "github.com/nalgeon/redka/internal/core"

// Removes the expiration time of a key.
// PERSIST key
// https://redis.io/commands/persist
type Persist struct {
	baseCmd
	key string
}

func parsePersist(b baseCmd) (*Persist, error) {
	cmd := &Persist{baseCmd: b}
	if len(cmd.args) != 1 {
		return cmd, ErrInvalidArgNum
	}
	cmd.key = string(cmd.args[0])
	return cmd, nil
}

func (cmd *Persist) Run(w Writer, red Redka) (any, error) {
	err := red.Key().Persist(cmd.key)
	if err != nil && err != core.ErrNotFound {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if err == core.ErrNotFound {
		w.WriteInt(0)
		return false, nil

	}
	w.WriteInt(1)
	return true, nil
}
