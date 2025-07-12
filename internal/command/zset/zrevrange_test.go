package zset

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/rzset"
)

func TestZRevRangeParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZRevRange
		err  error
	}{
		{
			cmd:  "zrevrange",
			want: ZRevRange{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrevrange key",
			want: ZRevRange{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrevrange key 11",
			want: ZRevRange{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrevrange key 11 22",
			want: ZRevRange{key: "key", start: 11, stop: 22},
			err:  nil,
		},
		{
			cmd:  "zrevrange key 11 22 withscores",
			want: ZRevRange{key: "key", start: 11, stop: 22, withScores: true},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZRevRange, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.start, test.want.start)
				be.Equal(t, cmd.stop, test.want.stop)
				be.Equal(t, cmd.withScores, test.want.withScores)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestZRevRangeExec(t *testing.T) {
	t.Run("range", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 1)
		_, _ = red.ZSet().Add("key", "two", 2)
		_, _ = red.ZSet().Add("key", "thr", 3)
		_, _ = red.ZSet().Add("key", "2nd", 2)

		{
			cmd := redis.MustParse(ParseZRevRange, "zrevrange key 0 1")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 2)
			be.Equal(t, conn.Out(), "2,thr,two")
		}
		{
			cmd := redis.MustParse(ParseZRevRange, "zrevrange key 0 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 4)
			be.Equal(t, conn.Out(), "4,thr,two,2nd,one")
		}
		{
			cmd := redis.MustParse(ParseZRevRange, "zrevrange key 3 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 1)
			be.Equal(t, conn.Out(), "1,one")
		}
		{
			cmd := redis.MustParse(ParseZRevRange, "zrevrange key 4 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 0)
			be.Equal(t, conn.Out(), "0")
		}
	})
	t.Run("with scores", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 1)
		_, _ = red.ZSet().Add("key", "two", 2)
		_, _ = red.ZSet().Add("key", "thr", 3)
		_, _ = red.ZSet().Add("key", "2nd", 2)

		cmd := redis.MustParse(ParseZRevRange, "zrevrange key 0 5 withscores")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]rzset.SetItem)), 4)
		be.Equal(t, conn.Out(), "8,thr,3,two,2,2nd,2,one,1")
	})
	t.Run("negative indexes", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 1)
		_, _ = red.ZSet().Add("key", "two", 2)
		_, _ = red.ZSet().Add("key", "thr", 3)
		_, _ = red.ZSet().Add("key", "2nd", 2)

		cmd := redis.MustParse(ParseZRevRange, "zrevrange key -2 -1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]rzset.SetItem)), 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseZRevRange, "zrevrange key 0 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]rzset.SetItem)), 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "value")

		cmd := redis.MustParse(ParseZRevRange, "zrevrange key 0 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]rzset.SetItem)), 0)
		be.Equal(t, conn.Out(), "0")
	})
}
