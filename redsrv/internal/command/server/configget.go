package server

import (
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

// Returns the effective values of configuration parameters.
// CONFIG GET parameter [parameter ...]
// https://redis.io/commands/config-get
type ConfigGet struct {
	params []string
}

func ParseConfigGet(args [][]byte) (ConfigGet, error) {
	if len(args) < 1 {
		return ConfigGet{}, redis.ErrInvalidArgNum
	}
	cmd := ConfigGet{params: make([]string, len(args))}
	for i, arg := range args {
		cmd.params[i] = string(arg)
	}
	return cmd, nil
}

func (c ConfigGet) Run(w redis.Writer, _ redis.Redka) (any, error) {
	w.WriteArray(2)
	w.WriteString("databases")
	w.WriteInt(1)
	return true, nil
}
