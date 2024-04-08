package command

import "github.com/nalgeon/redka/internal/core"

// Renames a key and overwrites the destination.
// RENAME key newkey
// https://redis.io/commands/rename
type Rename struct {
	baseCmd
	key    string
	newKey string
}

func parseRename(b baseCmd) (*Rename, error) {
	cmd := &Rename{baseCmd: b}
	if len(cmd.args) != 2 {
		return cmd, ErrInvalidArgNum(cmd.name)
	}
	cmd.key = string(cmd.args[0])
	cmd.newKey = string(cmd.args[1])
	return cmd, nil
}

func (cmd *Rename) Run(w Writer, red Redka) (any, error) {
	ok, err := red.Key().Rename(cmd.key, cmd.newKey)

	if err == core.ErrKeyNotFound {
		w.WriteError(ErrKeyNotFound.Error())
		return false, ErrKeyNotFound
	}
	if err != nil {
		w.WriteError(err.Error())
		return false, err
	}
	w.WriteString("OK")
	return ok, nil
}
