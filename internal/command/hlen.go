package command

import "github.com/nalgeon/redka"

// Returns the number of fields in a hash.
// HLEN key
// https://redis.io/commands/hlen
type HLen struct {
	baseCmd
	key string
}

func parseHLen(b baseCmd) (*HLen, error) {
	cmd := &HLen{baseCmd: b}
	if len(cmd.args) != 1 {
		return cmd, ErrInvalidArgNum
	}
	cmd.key = string(cmd.args[0])
	return cmd, nil
}

func (cmd *HLen) Run(w Writer, red *redka.Tx) (any, error) {
	count, err := red.Hash().Len(cmd.key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
