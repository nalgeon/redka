package server

import "github.com/nalgeon/redka/internal/redis"

// Unknown is a placeholder for unknown commands.
// Always returns an error.
type Unknown struct {
	redis.BaseCmd
}

func ParseUnknown(b redis.BaseCmd) (Unknown, error) {
	return Unknown{BaseCmd: b}, nil
}

func (cmd Unknown) Run(w redis.Writer, _ redis.Redka) (any, error) {
	err := redis.ErrUnknownCmd
	w.WriteError(cmd.Error(err))
	return nil, err
}
