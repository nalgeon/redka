package hash

import "github.com/nalgeon/redka/redsrv/internal/redis"

// Sets the value of a field in a hash only when the field doesn't exist.
// HSETNX key field value
// https://redis.io/commands/hsetnx
type HSetNX struct {
	redis.BaseCmd
	key   string
	field string
	value []byte
}

func ParseHSetNX(b redis.BaseCmd) (HSetNX, error) {
	cmd := HSetNX{BaseCmd: b}
	if len(cmd.Args()) != 3 {
		return HSetNX{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	cmd.field = string(cmd.Args()[1])
	cmd.value = cmd.Args()[2]
	return cmd, nil
}

func (cmd HSetNX) Run(w redis.Writer, red redis.Redka) (any, error) {
	ok, err := red.Hash().SetNotExists(cmd.key, cmd.field, cmd.value)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if ok {
		w.WriteInt(1)
	} else {
		w.WriteInt(0)
	}
	return ok, nil
}
