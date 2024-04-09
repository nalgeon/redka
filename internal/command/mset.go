package command

import (
	"github.com/nalgeon/redka"
)

// Atomically creates or modifies the string values of one or more keys.
// MSET key value [key value ...]
// https://redis.io/commands/mset
type MSet struct {
	baseCmd
	kvals []redka.KeyValue
}

func parseMSet(b baseCmd) (*MSet, error) {
	cmd := &MSet{baseCmd: b}
	if len(cmd.args) < 2 || len(cmd.args)%2 != 0 {
		return cmd, ErrInvalidArgNum(cmd.name)
	}

	cmd.kvals = make([]redka.KeyValue, len(cmd.args)/2)
	for i := 0; i < len(cmd.args); i += 2 {
		cmd.kvals[i/2] = redka.KeyValue{
			Key:   string(cmd.args[i]),
			Value: cmd.args[i+1],
		}
	}

	return cmd, nil
}

func (cmd *MSet) Run(w Writer, red Redka) (any, error) {
	err := red.Str().SetMany(cmd.kvals...)
	if err != nil {
		w.WriteError(err.Error())
		return nil, err
	}
	w.WriteString("OK")
	return true, nil
}
