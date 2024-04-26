package command

import "github.com/nalgeon/redka/internal/parser"

// Adds one or more members to a sorted set, or updates their scores.
// Creates the key if it doesn't exist.
// ZADD key score member [score member ...]
// https://redis.io/commands/zadd
type ZAdd struct {
	baseCmd
	key   string
	items map[any]float64
}

func parseZAdd(b baseCmd) (*ZAdd, error) {
	cmd := &ZAdd{baseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.FloatMap(&cmd.items),
	).Required(3).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZAdd) Run(w Writer, red Redka) (any, error) {
	count, err := red.ZSet().AddMany(cmd.key, cmd.items)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
