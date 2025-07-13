package zset

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestZRemParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZRem
		err  error
	}{
		{
			cmd:  "zrem",
			want: ZRem{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrem key",
			want: ZRem{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrem key member",
			want: ZRem{key: "key", members: []any{"member"}},
			err:  nil,
		},
		{
			cmd:  "zrem key one two thr",
			want: ZRem{key: "key", members: []any{"one", "two", "thr"}},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZRem, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.members, test.want.members)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestZRemExec(t *testing.T) {
	t.Run("some", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 1)
		_, _ = red.ZSet().Add("key", "two", 2)
		_, _ = red.ZSet().Add("key", "thr", 3)

		cmd := redis.MustParse(ParseZRem, "zrem key one two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		count, _ := red.ZSet().Len("key")
		be.Equal(t, count, 1)

		_, err = red.ZSet().GetScore("key", "one")
		be.Err(t, err, core.ErrNotFound)
		_, err = red.ZSet().GetScore("key", "two")
		be.Err(t, err, core.ErrNotFound)
	})
	t.Run("all", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 1)
		_, _ = red.ZSet().Add("key", "two", 2)
		_, _ = red.ZSet().Add("key", "thr", 3)

		cmd := redis.MustParse(ParseZRem, "zrem key one two thr")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 3)
		be.Equal(t, conn.Out(), "3")

		count, _ := red.ZSet().Len("key")
		be.Equal(t, count, 0)
	})
	t.Run("none", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 1)
		_, _ = red.ZSet().Add("key", "two", 2)
		_, _ = red.ZSet().Add("key", "thr", 3)

		cmd := redis.MustParse(ParseZRem, "zrem key fou fiv")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		count, _ := red.ZSet().Len("key")
		be.Equal(t, count, 3)
	})
	t.Run("key not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseZRem, "zrem key one two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "str")

		cmd := redis.MustParse(ParseZRem, "zrem key fou fiv")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		val, _ := red.Str().Get("key")
		be.Equal(t, val.String(), "str")
	})
}
