package server

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func getRedka(tb testing.TB) redis.Redka {
	tb.Helper()
	db := testx.OpenDB(tb)
	return redis.RedkaDB(db)
}
