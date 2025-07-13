package zset

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestZCountParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZCount
		err  error
	}{
		{
			cmd:  "zcount",
			want: ZCount{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zcount key",
			want: ZCount{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zcount key 1.1",
			want: ZCount{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zcount key 1.1 2.2",
			want: ZCount{key: "key", min: 1.1, max: 2.2},
			err:  nil,
		},
		{
			cmd:  "zcount key 1.1 2.2 3.3",
			want: ZCount{},
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZCount, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.min, test.want.min)
				be.Equal(t, cmd.max, test.want.max)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestZCountExec(t *testing.T) {
	t.Run("zcount", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 11)
		_, _ = red.ZSet().Add("key", "two", 22)
		_, _ = red.ZSet().Add("key", "thr", 33)

		cmd := redis.MustParse(ParseZCount, "zcount key 15 25")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 1)
		be.Equal(t, conn.Out(), "1")
	})
	t.Run("inclusive", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 11)
		_, _ = red.ZSet().Add("key", "two", 22)
		_, _ = red.ZSet().Add("key", "thr", 33)

		cmd := redis.MustParse(ParseZCount, "zcount key 11 33")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 3)
		be.Equal(t, conn.Out(), "3")
	})
	t.Run("zero", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 11)
		_, _ = red.ZSet().Add("key", "two", 22)
		_, _ = red.ZSet().Add("key", "thr", 33)

		cmd := redis.MustParse(ParseZCount, "zcount key 44 55")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseZCount, "zcount key 11 33")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "value")

		cmd := redis.MustParse(ParseZCount, "zcount key 11 33")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
}
