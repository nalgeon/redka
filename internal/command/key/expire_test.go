package key

import (
	"testing"
	"time"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
)

func TestExpireParse(t *testing.T) {
	tests := []struct {
		cmd string
		key string
		ttl time.Duration
		err error
	}{
		{
			cmd: "expire",
			key: "",
			ttl: 0,
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "expire name",
			key: "",
			ttl: 0,
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "expire name 60",
			key: "name",
			ttl: 60 * 1000 * time.Millisecond,
			err: nil,
		},
		{
			cmd: "expire name age",
			key: "",
			ttl: 0,
			err: redis.ErrInvalidInt,
		},
		{
			cmd: "expire name 60 age 60",
			key: "",
			ttl: 0,
			err: redis.ErrSyntaxError,
		},
	}

	parse := func(b redis.BaseCmd) (Expire, error) {
		return ParseExpire(b, 1000)
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(parse, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.key)
				be.Equal(t, cmd.ttl, test.ttl)
			} else {
				be.Equal(t, cmd, Expire{})
			}
		})
	}
}

func TestExpireExec(t *testing.T) {
	parse := func(b redis.BaseCmd) (Expire, error) {
		return ParseExpire(b, 1000)
	}
	t.Run("create expire", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := redis.MustParse(parse, "expire name 60")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "1")

		expireAt := time.Now().Add(60 * time.Second)
		key, _ := db.Key().Get("name")
		be.Equal(t, *key.ETime/1000, expireAt.UnixMilli()/1000)
	})

	t.Run("update expire", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().SetExpires("name", "alice", 60*time.Second)

		cmd := redis.MustParse(parse, "expire name 30")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "1")

		expireAt := time.Now().Add(30 * time.Second)
		key, _ := db.Key().Get("name")
		be.Equal(t, *key.ETime/1000, expireAt.UnixMilli()/1000)
	})

	t.Run("set to zero", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := redis.MustParse(parse, "expire name 0")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "1")

		key, _ := db.Key().Get("name")
		be.Equal(t, key.Exists(), false)
	})

	t.Run("negative", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := redis.MustParse(parse, "expire name -10")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "1")

		key, _ := db.Key().Get("name")
		be.Equal(t, key.Exists(), false)
	})

	t.Run("not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := redis.MustParse(parse, "expire age 60")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, false)
		be.Equal(t, conn.Out(), "0")

		key, _ := db.Key().Get("age")
		be.Equal(t, key.Exists(), false)
	})
}
