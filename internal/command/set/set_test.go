package set

import (
	"slices"
	"sort"
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func getRedka(tb testing.TB) redis.Redka {
	tb.Helper()
	db := testx.OpenDB(tb)
	return redis.RedkaDB(db)
}

func sortValues(vals []core.Value) {
	sort.Slice(vals, func(i, j int) bool {
		return slices.Compare(vals[i], vals[j]) < 0
	})
}
