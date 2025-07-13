package server

import "github.com/nalgeon/redka/redsrv/internal/redis"

// Returns the number of keys in the database.
// DBSIZE
// https://redis.io/commands/dbsize
type DBSize struct {
	redis.BaseCmd
}

func ParseDBSize(b redis.BaseCmd) (DBSize, error) {
	cmd := DBSize{BaseCmd: b}
	if len(cmd.Args()) != 0 {
		return DBSize{}, redis.ErrInvalidArgNum
	}
	return cmd, nil
}

func (cmd DBSize) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.Key().Len()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
