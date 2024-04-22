package command

import (
	"strconv"

	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/sqlx"
)

// Returns the intersect of multiple sorted sets.
// ZINTER numkeys key [key ...] [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]
// https://redis.io/commands/zinter
type ZInter struct {
	baseCmd
	keys       []string
	aggregate  string
	withScores bool
}

func parseZInter(b baseCmd) (*ZInter, error) {
	cmd := &ZInter{baseCmd: b}
	if len(cmd.args) < 2 {
		return nil, ErrInvalidArgNum
	}
	nKeys, err := strconv.Atoi(string(cmd.args[0]))
	if err != nil {
		return nil, ErrInvalidInt
	}
	err = parser.New(
		parser.Int(&nKeys),
		parser.StringsN(&cmd.keys, nKeys),
		parser.Named("aggregate", parser.Enum(&cmd.aggregate, sqlx.Sum, sqlx.Min, sqlx.Max)),
		parser.Flag("withscores", &cmd.withScores),
	).Required(2).Run(cmd.args)
	if err != nil {
		return nil, err
	}

	return cmd, nil
}

func (cmd *ZInter) Run(w Writer, red Redka) (any, error) {
	inter := red.ZSet().InterWith(cmd.keys...)
	switch cmd.aggregate {
	case sqlx.Min:
		inter = inter.Min()
	case sqlx.Max:
		inter = inter.Max()
	case sqlx.Sum:
		inter = inter.Sum()
	}

	items, err := inter.Run()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	if cmd.withScores {
		w.WriteArray(len(items) * 2)
		for _, item := range items {
			w.WriteBulk(item.Elem)
			w.WriteBulkString(floatToString(item.Score))
		}
	} else {
		w.WriteArray(len(items))
		for _, item := range items {
			w.WriteBulk(item.Elem)
		}
	}

	return items, nil
}
