package command

import "github.com/nalgeon/redka/internal/parser"

// Increments the score of a member in a sorted set.
// ZINCRBY key increment member
// https://redis.io/commands/zincrby
type ZIncrBy struct {
	baseCmd
	key    string
	delta  float64
	member string
}

func parseZIncrBy(b baseCmd) (*ZIncrBy, error) {
	cmd := &ZIncrBy{baseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&cmd.delta),
		parser.String(&cmd.member),
	).Required(3).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZIncrBy) Run(w Writer, red Redka) (any, error) {
	score, err := red.ZSet().Incr(cmd.key, cmd.member, cmd.delta)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	writeFloat(w, score)
	return score, nil
}
