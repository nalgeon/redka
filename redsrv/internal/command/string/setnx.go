package string

import "github.com/nalgeon/redka/redsrv/internal/redis"

// Set the string value of a key only when the key doesn't exist.
// SETNX key value
// https://redis.io/commands/setnx
type SetNX struct {
	redis.BaseCmd
	key   string
	value []byte
}

func ParseSetNX(b redis.BaseCmd) (SetNX, error) {
	cmd := SetNX{BaseCmd: b}
	if len(cmd.Args()) != 2 {
		return SetNX{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	cmd.value = cmd.Args()[1]
	return cmd, nil
}

func (cmd SetNX) Run(w redis.Writer, red redis.Redka) (any, error) {
	out, err := red.Str().SetWith(cmd.key, cmd.value).IfNotExists().Run()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if out.Created {
		w.WriteInt(1)
	} else {
		w.WriteInt(0)
	}
	return out.Created, nil
}
