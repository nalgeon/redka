package string

import (
	"testing"
	"time"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestSetEXParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SetEX
		err  error
	}{
		{
			cmd:  "setex",
			want: SetEX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "setex name",
			want: SetEX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "setex name alice",
			want: SetEX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "setex name alice 60",
			want: SetEX{},
			err:  redis.ErrInvalidInt,
		},
		{
			cmd:  "setex name 60 alice",
			want: SetEX{key: "name", value: []byte("alice"), ttl: 60 * 1000 * time.Millisecond},
			err:  nil,
		},
	}

	parse := func(b redis.BaseCmd) (SetEX, error) {
		return ParseSetEX(b, 1000)
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(parse, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.value, test.want.value)
				be.Equal(t, cmd.ttl, test.want.ttl)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestSetEXExec(t *testing.T) {
	parse := func(b redis.BaseCmd) (SetEX, error) {
		return ParseSetEX(b, 1000)
	}

	t.Run("create", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(parse, "setex name 60 alice")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "OK")

		expireAt := time.Now().Add(60 * time.Second)
		key, _ := red.Key().Get("name")
		be.Equal(t, *key.ETime/1000, expireAt.UnixMilli()/1000)

		name, _ := red.Str().Get("name")
		be.Equal(t, name.String(), "alice")
	})

	t.Run("update", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("name", "alice")

		cmd := redis.MustParse(parse, "setex name 60 bob")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "OK")

		expireAt := time.Now().Add(60 * time.Second)
		key, _ := red.Key().Get("name")
		be.Equal(t, *key.ETime/1000, expireAt.UnixMilli()/1000)

		name, _ := red.Str().Get("name")
		be.Equal(t, name.String(), "bob")
	})

	t.Run("change ttl", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().SetExpire("name", "alice", 60*time.Second)

		cmd := redis.MustParse(parse, "setex name 10 bob")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "OK")

		expireAt := time.Now().Add(10 * time.Second)
		key, _ := red.Key().Get("name")
		be.Equal(t, *key.ETime/1000, expireAt.UnixMilli()/1000)

		name, _ := red.Str().Get("name")
		be.Equal(t, name.String(), "bob")
	})

}
