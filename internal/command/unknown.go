package command

import (
	"github.com/nalgeon/redka"
	"github.com/tidwall/redcon"
)

// Unknown is a placeholder for unknown commands.
// Always returns an error.
type Unknown struct {
	baseCmd
}

func parseUnknown(b baseCmd) (*Unknown, error) {
	return &Unknown{baseCmd: b}, nil
}

func (cmd *Unknown) Run(w redcon.Conn, _ redka.Redka) (any, error) {
	err := ErrUnknownCmd(cmd.name)
	w.WriteError(err.Error())
	return false, err
}
