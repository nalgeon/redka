package command

import "github.com/nalgeon/redka/internal/core"

// Returns a random key name from the database.
// RANDOMKEY
// https://redis.io/commands/randomkey
type RandomKey struct {
	baseCmd
}

func parseRandomKey(b baseCmd) (*RandomKey, error) {
	cmd := &RandomKey{baseCmd: b}
	if len(cmd.args) != 0 {
		return cmd, ErrInvalidArgNum
	}
	return cmd, nil
}

func (cmd *RandomKey) Run(w Writer, red Redka) (any, error) {
	key, err := red.Key().Random()
	if err == core.ErrNotFound {
		w.WriteNull()
		return nil, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteBulkString(key.Key)
	return key, nil
}
