package string

import "github.com/nalgeon/redka/internal/redis"

// Returns the previous string value of a key after setting it to a new value.
// GETSET key value
// https://redis.io/commands/getset
type GetSet struct {
	redis.BaseCmd
	Key   string
	Value []byte
}

func ParseGetSet(b redis.BaseCmd) (*GetSet, error) {
	cmd := &GetSet{BaseCmd: b}
	if len(cmd.Args()) != 2 {
		return cmd, redis.ErrInvalidArgNum
	}
	cmd.Key = string(cmd.Args()[0])
	cmd.Value = cmd.Args()[1]
	return cmd, nil
}

func (cmd *GetSet) Run(w redis.Writer, red redis.Redka) (any, error) {
	out, err := red.Str().SetWith(cmd.Key, cmd.Value).Run()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if !out.Prev.Exists() {
		w.WriteNull()
		return out.Prev, nil
	}
	w.WriteBulk(out.Prev)
	return out.Prev, nil
}
