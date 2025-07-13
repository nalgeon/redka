package key

import (
	"testing"
	"time"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
)

func TestPExpireParse(t *testing.T) {
	tests := []struct {
		cmd string
		key string
		ttl time.Duration
		err error
	}{
		{
			cmd: "pexpire",
			key: "",
			ttl: 0,
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "pexpire name",
			key: "",
			ttl: 0,
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "pexpire name 5000",
			key: "name",
			ttl: 5000 * time.Millisecond,
			err: nil,
		},
		{
			cmd: "pexpire name age",
			key: "",
			ttl: 0,
			err: redis.ErrInvalidInt,
		},
		{
			cmd: "pexpire name 100 age 100",
			key: "",
			ttl: 0,
			err: redis.ErrSyntaxError,
		},
	}

	parse := func(b redis.BaseCmd) (Expire, error) {
		return ParseExpire(b, 1)
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

func TestPExpireExec(t *testing.T) {
	parse := func(b redis.BaseCmd) (Expire, error) {
		return ParseExpire(b, 1)
	}

	t.Run("create pexpire", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("name", "alice")

		cmd := redis.MustParse(parse, "pexpire name 60000")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "1")

		expireAt := time.Now().Add(60 * time.Second)
		key, _ := red.Key().Get("name")
		be.Equal(t, *key.ETime/1000, expireAt.UnixMilli()/1000)
	})

	t.Run("update pexpire", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().SetExpire("name", "alice", 60*time.Second)

		cmd := redis.MustParse(parse, "pexpire name 30000")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "1")

		expireAt := time.Now().Add(30 * time.Second)
		key, _ := red.Key().Get("name")
		be.Equal(t, *key.ETime/1000, expireAt.UnixMilli()/1000)
	})

	t.Run("set to zero", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("name", "alice")

		cmd := redis.MustParse(parse, "pexpire name 0")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "1")

		key, _ := red.Key().Get("name")
		be.Equal(t, key.Exists(), false)
	})

	t.Run("negative", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("name", "alice")

		cmd := redis.MustParse(parse, "pexpire name -1000")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "1")

		key, _ := red.Key().Get("name")
		be.Equal(t, key.Exists(), false)
	})

	t.Run("not found", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("name", "alice")

		cmd := redis.MustParse(parse, "pexpire age 1000")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, false)
		be.Equal(t, conn.Out(), "0")

		key, _ := red.Key().Get("age")
		be.Equal(t, key.Exists(), false)
	})
}
