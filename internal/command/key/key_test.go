package key

import (
	"testing"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/redis"
)

func getDB(tb testing.TB) (*redka.DB, redis.Redka) {
	tb.Helper()
	db, err := redka.Open("file:/data.db?vfs=memdb", nil)
	if err != nil {
		tb.Fatal(err)
	}
	return db, redis.RedkaDB(db)
}
