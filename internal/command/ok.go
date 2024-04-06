package command

import (
	"github.com/nalgeon/redka"
	"github.com/tidwall/redcon"
)

// Dummy command that always returns OK.
type OK struct {
	baseCmd
}

func parseOK(b baseCmd) (*OK, error) {
	return &OK{baseCmd: b}, nil
}

func (c *OK) Run(w redcon.Conn, _ redka.Redka) (any, error) {
	w.WriteString("OK")
	return true, nil
}
