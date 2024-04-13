package command

import "github.com/nalgeon/redka"

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

func (cmd *RandomKey) Run(w Writer, red *redka.Tx) (any, error) {
	key, err := red.Key().Random()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if !key.Exists() {
		w.WriteNull()
		return nil, nil
	}
	w.WriteBulkString(key.Key)
	return key, nil
}
