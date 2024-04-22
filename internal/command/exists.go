package command

import "github.com/nalgeon/redka/internal/parser"

// Determines whether one or more keys exist.
// EXISTS key [key ...]
// https://redis.io/commands/exists
type Exists struct {
	baseCmd
	keys []string
}

func parseExists(b baseCmd) (*Exists, error) {
	cmd := &Exists{baseCmd: b}
	err := parser.New(
		parser.Strings(&cmd.keys),
	).Required(1).Run(cmd.args)
	if err != nil {
		return cmd, err
	}
	return cmd, nil
}

func (cmd *Exists) Run(w Writer, red Redka) (any, error) {
	count, err := red.Key().Count(cmd.keys...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
