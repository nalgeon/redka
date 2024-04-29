package zset_test

import (
	"testing"

	"github.com/nalgeon/redka"
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
