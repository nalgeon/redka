package key

import "github.com/nalgeon/redka/internal/redis"

// Remove all keys from the current database.
// FLUSHDB
// https://redis.io/commands/flushdb
type FlushDB struct {
	redis.BaseCmd
}

func ParseFlushDB(b redis.BaseCmd) (*FlushDB, error) {
	cmd := &FlushDB{BaseCmd: b}
	if len(cmd.Args()) != 0 {
		return cmd, redis.ErrSyntaxError
	}
	return cmd, nil
}

func (cmd *FlushDB) Run(w redis.Writer, red redis.Redka) (any, error) {
	err := red.Key().DeleteAll()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return true, nil
}
