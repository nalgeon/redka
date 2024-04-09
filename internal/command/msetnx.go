package command

import (
	"github.com/nalgeon/redka"
)

// Atomically modifies the string values of one
// or more keys only when all keys don't exist.
// MSETNX key value [key value ...]
// https://redis.io/commands/msetnx
type MSetNX struct {
	baseCmd
	kvals []redka.KeyValue
}

func parseMSetNX(b baseCmd) (*MSetNX, error) {
	cmd := &MSetNX{baseCmd: b}
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

func (cmd *MSetNX) Run(w Writer, red Redka) (any, error) {
	ok, err := red.Str().SetManyNX(cmd.kvals...)
	if err != nil {
		w.WriteError(err.Error())
		return nil, err
	}
	if ok {
		w.WriteInt(1)
	} else {
		w.WriteInt(0)
	}
	return ok, nil
}
