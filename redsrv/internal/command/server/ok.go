package server

import "github.com/nalgeon/redka/redsrv/internal/redis"

// Dummy command that always returns OK.
type OK struct {
	redis.BaseCmd
}

func ParseOK(b redis.BaseCmd) (OK, error) {
	return OK{BaseCmd: b}, nil
}

func (c OK) Run(w redis.Writer, _ redis.Redka) (any, error) {
	w.WriteString("OK")
	return true, nil
}
