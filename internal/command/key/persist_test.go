package key

import (
	"testing"
	"time"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
)

func TestPersistParse(t *testing.T) {
	tests := []struct {
		cmd string
		key string
		err error
	}{
		{
			cmd: "persist",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "persist name",
			key: "name",
			err: nil,
		},
		{
			cmd: "persist name age",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParsePersist, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.key)
			} else {
				be.Equal(t, cmd, Persist{})
			}
		})
	}
}

func TestPersistExec(t *testing.T) {
	t.Run("persist to persist", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := redis.MustParse(ParsePersist, "persist name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "1")

		key, _ := db.Key().Get("name")
		be.Equal(t, key.ETime, (*int64)(nil))
	})

	t.Run("volatile to persist", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().SetExpires("name", "alice", 60*time.Second)

		cmd := redis.MustParse(ParsePersist, "persist name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "1")

		key, _ := db.Key().Get("name")
		be.Equal(t, key.ETime, (*int64)(nil))
	})

	t.Run("not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := redis.MustParse(ParsePersist, "persist age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, false)
		be.Equal(t, conn.Out(), "0")

		key, _ := db.Key().Get("age")
		be.Equal(t, key.Exists(), false)
	})
}
