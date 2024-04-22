package command

import "github.com/nalgeon/redka/internal/parser"

// Deletes one or more fields and their values from a hash.
// Deletes the hash if no fields remain.
// HDEL key field [field ...]
// https://redis.io/commands/hdel
type HDel struct {
	baseCmd
	key    string
	fields []string
}

func parseHDel(b baseCmd) (*HDel, error) {
	cmd := &HDel{baseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Strings(&cmd.fields),
	).Required(2).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *HDel) Run(w Writer, red Redka) (any, error) {
	count, err := red.Hash().Delete(cmd.key, cmd.fields...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
