package server

import (
	"github.com/nalgeon/redka/internal/redis"
)

// Container command for runtime configuration commands.
// CONFIG
// https://redis.io/commands/config
type Config struct {
	redis.BaseCmd
	subcmd string
	get    ConfigGet
}

func ParseConfig(b redis.BaseCmd) (Config, error) {
	// Extract the subcommand.
	cmd := Config{BaseCmd: b}
	if len(cmd.Args()) == 0 {
		return Config{}, redis.ErrInvalidArgNum
	}
	cmd.subcmd = string(cmd.Args()[0])

	// Parse the subcommand.
	var err error
	args := cmd.Args()[1:]
	switch cmd.subcmd {
	case "get":
		cmd.get, err = ParseConfigGet(args)
	default:
		err = redis.ErrUnknownSubcmd
	}

	// Return the resulting command.
	if err != nil {
		return Config{}, err
	}
	return cmd, nil
}

func (c Config) Run(w redis.Writer, red redis.Redka) (any, error) {
	switch c.subcmd {
	case "get":
		return c.get.Run(w, red)
	default:
		w.WriteString("OK")
		return true, nil
	}
}
