package key

import "github.com/nalgeon/redka/redsrv/internal/redis"

// Renames a key and overwrites the destination.
// RENAME key newkey
// https://redis.io/commands/rename
type Rename struct {
	redis.BaseCmd
	key    string
	newKey string
}

func ParseRename(b redis.BaseCmd) (Rename, error) {
	cmd := Rename{BaseCmd: b}
	if len(cmd.Args()) != 2 {
		return Rename{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	cmd.newKey = string(cmd.Args()[1])
	return cmd, nil
}

func (cmd Rename) Run(w redis.Writer, red redis.Redka) (any, error) {
	err := red.Key().Rename(cmd.key, cmd.newKey)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return true, nil
}
