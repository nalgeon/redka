package command

import "github.com/nalgeon/redka"

// Dummy command that always returns OK.
type OK struct {
	baseCmd
}

func parseOK(b baseCmd) (*OK, error) {
	return &OK{baseCmd: b}, nil
}

func (c *OK) Run(w Writer, _ *redka.Tx) (any, error) {
	w.WriteString("OK")
	return true, nil
}
