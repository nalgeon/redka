package command

import (
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/parser"
)

// Returns the index of a member in a sorted set ordered by descending scores.
// ZREVRANK key member [WITHSCORE]
// https://redis.io/commands/zrevrank
type ZRevRank struct {
	baseCmd
	key       string
	member    string
	withScore bool
}

func parseZRevRank(b baseCmd) (*ZRevRank, error) {
	cmd := &ZRevRank{baseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.String(&cmd.member),
		parser.Flag("withscore", &cmd.withScore),
	).Required(2).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZRevRank) Run(w Writer, red Redka) (any, error) {
	rank, score, err := red.ZSet().GetRankRev(cmd.key, cmd.member)
	if err == core.ErrNotFound {
		w.WriteNull()
		return nil, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if cmd.withScore {
		w.WriteArray(2)
		w.WriteInt(rank)
		writeFloat(w, score)
		return rank, nil
	}
	w.WriteInt(rank)
	return rank, nil
}
