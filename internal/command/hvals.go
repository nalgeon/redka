package command

import "github.com/nalgeon/redka"

// Returns all values in a hash.
// HVALS key
// https://redis.io/commands/hvals
type HVals struct {
	baseCmd
	key string
}

func parseHVals(b baseCmd) (*HVals, error) {
	cmd := &HVals{baseCmd: b}
	if len(cmd.args) != 1 {
		return cmd, ErrInvalidArgNum
	}
	cmd.key = string(cmd.args[0])
	return cmd, nil
}

func (cmd *HVals) Run(w Writer, red *redka.Tx) (any, error) {
	vals, err := red.Hash().Values(cmd.key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteArray(len(vals))
	for _, val := range vals {
		w.WriteBulk(val)
	}
	return vals, nil
}
