package set

import (
	"slices"
	"sort"
	"testing"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

func getDB(tb testing.TB) (*redka.DB, redis.Redka) {
	tb.Helper()
	db, err := redka.Open(":memory:", nil)
	if err != nil {
		tb.Fatal(err)
	}
	return db, redis.RedkaDB(db)
}

func sortValues(vals []core.Value) {
	sort.Slice(vals, func(i, j int) bool {
		return slices.Compare(vals[i], vals[j]) < 0
	})
}
