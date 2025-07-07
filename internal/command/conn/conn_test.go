package conn

import (
	"testing"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func getDB(tb testing.TB) (*redka.DB, redis.Redka) {
	tb.Helper()
	db := testx.OpenDB(tb)
	return db, redis.RedkaDB(db)
}
