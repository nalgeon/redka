package command

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/parser"
)

// Returns the score of a member in a sorted set.
// ZSCORE key member
// https://redis.io/commands/zscore
type ZScore struct {
	baseCmd
	key    string
	member string
}

func parseZScore(b baseCmd) (*ZScore, error) {
	cmd := &ZScore{baseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.String(&cmd.member),
	).Required(2).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZScore) Run(w Writer, red Redka) (any, error) {
	score, err := red.ZSet().GetScore(cmd.key, cmd.member)
	if err == core.ErrNotFound {
		w.WriteNull()
		return nil, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	writeFloat(w, score)
	return score, nil
}
