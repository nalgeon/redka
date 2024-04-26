package command

import (
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/sqlx"
)

// Returns the union of multiple sorted sets.
// ZUNION numkeys key [key ...] [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]
// https://redis.io/commands/zunion
type ZUnion struct {
	baseCmd
	keys       []string
	aggregate  string
	withScores bool
}

func parseZUnion(b baseCmd) (*ZUnion, error) {
	cmd := &ZUnion{baseCmd: b}
	var nKeys int
	err := parser.New(
		parser.Int(&nKeys),
		parser.StringsN(&cmd.keys, &nKeys),
		parser.Named("aggregate", parser.Enum(&cmd.aggregate, sqlx.Sum, sqlx.Min, sqlx.Max)),
		parser.Flag("withscores", &cmd.withScores),
	).Required(2).Run(cmd.args)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (cmd *ZUnion) Run(w Writer, red Redka) (any, error) {
	union := red.ZSet().UnionWith(cmd.keys...)
	switch cmd.aggregate {
	case sqlx.Min:
		union = union.Min()
	case sqlx.Max:
		union = union.Max()
	case sqlx.Sum:
		union = union.Sum()
	}

	items, err := union.Run()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	if cmd.withScores {
		w.WriteArray(len(items) * 2)
		for _, item := range items {
			w.WriteBulk(item.Elem)
			writeFloat(w, item.Score)
		}
	} else {
		w.WriteArray(len(items))
		for _, item := range items {
			w.WriteBulk(item.Elem)
		}
	}

	return items, nil
}
