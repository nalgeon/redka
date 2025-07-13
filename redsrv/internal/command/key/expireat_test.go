package key

import (
	"fmt"
	"testing"
	"time"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestExpireAtParse(t *testing.T) {
	tests := []struct {
		cmd string
		key string
		at  time.Time
		err error
	}{
		{
			cmd: "expireat",
			key: "",
			at:  time.Time{},
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "expireat name",
			key: "",
			at:  time.Time{},
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "expireat name 60",
			key: "name",
			at:  time.UnixMilli(60 * 1000),
			err: nil,
		},
		{
			cmd: "expireat name age",
			key: "",
			at:  time.Time{},
			err: redis.ErrInvalidInt,
		},
		{
			cmd: "expireat name 60 age 60",
			key: "",
			at:  time.Time{},
			err: redis.ErrSyntaxError,
		},
	}

	parse := func(b redis.BaseCmd) (ExpireAt, error) {
		return ParseExpireAt(b, 1000)
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(parse, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.key)
				be.Equal(t, cmd.at.Unix(), test.at.Unix())
			} else {
				be.Equal(t, cmd, ExpireAt{})
			}
		})
	}
}

func TestExpireAtExec(t *testing.T) {
	parse := func(b redis.BaseCmd) (ExpireAt, error) {
		return ParseExpireAt(b, 1000)
	}
	t.Run("create expireat", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("name", "alice")

		expireAt := time.Now().Add(60 * time.Second)
		cmd := redis.MustParse(parse, fmt.Sprintf("expireat name %d", expireAt.Unix()))
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "1")

		key, _ := red.Key().Get("name")
		be.Equal(t, *key.ETime/1000, expireAt.UnixMilli()/1000)
	})

	t.Run("update expire", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("name", "alice")

		expireAt := time.Now()
		cmd := redis.MustParse(parse, fmt.Sprintf("expireat name %d", expireAt.Add(60*time.Second).Unix()))
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "1")

		cmd = redis.MustParse(parse, fmt.Sprintf("expireat name %d", expireAt.Add(20*time.Second).Unix()))
		conn = redis.NewFakeConn()
		res, err = cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "1")

		key, _ := red.Key().Get("name")
		be.Equal(t, *key.ETime/1000, expireAt.Add(20*time.Second).UnixMilli()/1000)
	})

	t.Run("set to zero", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("name", "alice")

		cmd := redis.MustParse(parse, "expireat name 0")
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

		cmd := redis.MustParse(parse, "expireat name -10")
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

		cmd := redis.MustParse(parse, "expireat age 1700000000")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, false)
		be.Equal(t, conn.Out(), "0")

		key, _ := red.Key().Get("age")
		be.Equal(t, key.Exists(), false)
	})
}
