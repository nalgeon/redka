package zset

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func getRedka(tb testing.TB) redis.Redka {
	tb.Helper()
	db := testx.OpenDB(tb)
	return redis.RedkaDB(db)
}
