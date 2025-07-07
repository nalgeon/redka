package key

import (
	"slices"
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

func TestRandomKeyParse(t *testing.T) {
	tests := []struct {
		cmd string
		err error
	}{
		{
			cmd: "randomkey",
			err: nil,
		},
		{
			cmd: "randomkey name",
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "randomkey name age",
			err: redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseRandomKey, test.cmd)
			be.Equal(t, err, test.err)
			if err != nil {
				be.Equal(t, cmd, RandomKey{})
			}
		})
	}
}

func TestRandomKeyExec(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)
		_ = db.Str().Set("city", "paris")
		keys := []string{"name", "age", "city"}

		conn := redis.NewFakeConn()
		cmd := redis.MustParse(ParseRandomKey, "randomkey")
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, slices.Contains(keys, res.(core.Key).Key), true)
		be.Equal(t, slices.Contains(keys, conn.Out()), true)
	})
	t.Run("not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		conn := redis.NewFakeConn()
		cmd := redis.MustParse(ParseRandomKey, "randomkey")
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), "(nil)")
	})
}
