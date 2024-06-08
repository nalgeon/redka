package key

import "github.com/nalgeon/redka/internal/redis"

// Renames a key only when the target key name doesn't exist.
// RENAMENX key newkey
// https://redis.io/commands/renamenx
type RenameNX struct {
	redis.BaseCmd
	key    string
	newKey string
}

func ParseRenameNX(b redis.BaseCmd) (RenameNX, error) {
	cmd := RenameNX{BaseCmd: b}
	if len(cmd.Args()) != 2 {
		return RenameNX{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	cmd.newKey = string(cmd.Args()[1])
	return cmd, nil
}

func (cmd RenameNX) Run(w redis.Writer, red redis.Redka) (any, error) {
	ok, err := red.Key().RenameNotExists(cmd.key, cmd.newKey)
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
