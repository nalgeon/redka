package command

import (
	"github.com/nalgeon/redka"
	"github.com/tidwall/redcon"
)

// Config is a container command for runtime configuration commands.
// This is a no-op implementation.
//
// CONFIG GET parameter [parameter ...]
// https://redis.io/commands/config-get
// CONFIG SET parameter value [parameter value ...]
// https://redis.io/commands/config-set
// CONFIG RESETSTAT
// https://redis.io/commands/config-resetstat
// CONFIG REWRITE
// https://redis.io/commands/config-rewrite
type Config struct {
	baseCmd
	subcmd string
	param  string
}

func parseConfig(b baseCmd) (*Config, error) {
	cmd := &Config{baseCmd: b}
	if len(b.args) < 1 {
		return cmd, ErrInvalidArgNum(b.name)
	}
	cmd.subcmd = string(b.args[0])
	switch cmd.subcmd {
	case "get":
		cmd.param = string(b.args[1])
	case "set":
		cmd.param = string(b.args[1])
	case "resetstat":
		// no-op
	case "rewrite":
		// no-op
	default:
		return cmd, ErrUnknownSubcmd(b.name, cmd.subcmd)
	}
	return cmd, nil
}

func (c *Config) Run(w redcon.Conn, _ redka.Redka) (any, error) {
	switch c.subcmd {
	case "get":
		w.WriteArray(2)
		w.WriteBulkString(c.param)
		w.WriteBulkString("")
	case "set":
		w.WriteString("OK")
	case "resetstat":
		w.WriteString("OK")
	case "rewrite":
		w.WriteString("OK")
	default:
		w.WriteString("OK")
	}
	return true, nil
}
