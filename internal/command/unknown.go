package command

import "github.com/nalgeon/redka"

// Unknown is a placeholder for unknown commands.
// Always returns an error.
type Unknown struct {
	baseCmd
}

func parseUnknown(b baseCmd) (*Unknown, error) {
	return &Unknown{baseCmd: b}, nil
}

func (cmd *Unknown) Run(w Writer, _ *redka.Tx) (any, error) {
	err := ErrUnknownCmd
	w.WriteError(cmd.Error(err))
	return false, err
}
