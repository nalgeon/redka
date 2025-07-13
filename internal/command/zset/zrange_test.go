package zset

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/rzset"
)

func TestZRangeParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZRange
		err  error
	}{
		{
			cmd:  "zrange",
			want: ZRange{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrange key",
			want: ZRange{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrange key 11",
			want: ZRange{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrange key 11 22",
			want: ZRange{key: "key", start: 11.0, stop: 22.0},
			err:  nil,
		},
		{
			cmd:  "zrange key 1.1 2.2 byscore",
			want: ZRange{key: "key", start: 1.1, stop: 2.2, byScore: true},
			err:  nil,
		},
		{
			cmd:  "zrange key (1 (2 byscore",
			want: ZRange{},
			err:  redis.ErrInvalidFloat,
		},
		{
			cmd:  "zrange key 11 22 rev",
			want: ZRange{key: "key", start: 11.0, stop: 22.0, rev: true},
			err:  nil,
		},
		{
			cmd:  "zrange key 11 22 byscore limit 10",
			want: ZRange{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd:  "zrange key 11 22 byscore limit 10 5",
			want: ZRange{key: "key", start: 11.0, stop: 22.0, byScore: true, offset: 10, count: 5},
			err:  nil,
		},
		{
			cmd:  "zrange key 11 22 withscores",
			want: ZRange{key: "key", start: 11.0, stop: 22.0, withScores: true},
			err:  nil,
		},
		{
			cmd: "zrange key 11 22 limit 10 5 rev byscore withscores",
			want: ZRange{
				key: "key", start: 11.0, stop: 22.0,
				byScore: true, rev: true,
				offset: 10, count: 5,
				withScores: true,
			},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZRange, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.start, test.want.start)
				be.Equal(t, cmd.stop, test.want.stop)
				be.Equal(t, cmd.byScore, test.want.byScore)
				be.Equal(t, cmd.rev, test.want.rev)
				be.Equal(t, cmd.offset, test.want.offset)
				be.Equal(t, cmd.count, test.want.count)
				be.Equal(t, cmd.withScores, test.want.withScores)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestZRangeExec(t *testing.T) {
	t.Run("by rank", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 1)
		_, _ = red.ZSet().Add("key", "two", 2)
		_, _ = red.ZSet().Add("key", "thr", 3)
		_, _ = red.ZSet().Add("key", "2nd", 2)

		{
			cmd := redis.MustParse(ParseZRange, "zrange key 0 1")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 2)
			be.Equal(t, conn.Out(), "2,one,2nd")
		}
		{
			cmd := redis.MustParse(ParseZRange, "zrange key 0 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 4)
			be.Equal(t, conn.Out(), "4,one,2nd,two,thr")
		}
		{
			cmd := redis.MustParse(ParseZRange, "zrange key 3 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 1)
			be.Equal(t, conn.Out(), "1,thr")
		}
		{
			cmd := redis.MustParse(ParseZRange, "zrange key 4 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 0)
			be.Equal(t, conn.Out(), "0")
		}
	})
	t.Run("by rank rev", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 1)
		_, _ = red.ZSet().Add("key", "two", 2)
		_, _ = red.ZSet().Add("key", "thr", 3)
		_, _ = red.ZSet().Add("key", "2nd", 2)

		{
			cmd := redis.MustParse(ParseZRange, "zrange key 0 1 rev")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 2)
			be.Equal(t, conn.Out(), "2,thr,two")
		}
		{
			cmd := redis.MustParse(ParseZRange, "zrange key 0 5 rev")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 4)
			be.Equal(t, conn.Out(), "4,thr,two,2nd,one")
		}
		{
			cmd := redis.MustParse(ParseZRange, "zrange key 3 5 rev")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 1)
			be.Equal(t, conn.Out(), "1,one")
		}
		{
			cmd := redis.MustParse(ParseZRange, "zrange key 4 5 rev")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 0)
			be.Equal(t, conn.Out(), "0")
		}
	})
	t.Run("by score", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 10)
		_, _ = red.ZSet().Add("key", "two", 20)
		_, _ = red.ZSet().Add("key", "thr", 30)
		_, _ = red.ZSet().Add("key", "2nd", 20)

		{
			cmd := redis.MustParse(ParseZRange, "zrange key 0 10 byscore")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 1)
			be.Equal(t, conn.Out(), "1,one")
		}
		{
			cmd := redis.MustParse(ParseZRange, "zrange key 0 50 byscore")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 4)
			be.Equal(t, conn.Out(), "4,one,2nd,two,thr")
		}
		{
			cmd := redis.MustParse(ParseZRange, "zrange key 30 50 byscore")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 1)
			be.Equal(t, conn.Out(), "1,thr")
		}
		{
			cmd := redis.MustParse(ParseZRange, "zrange key 40 50 byscore")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 0)
			be.Equal(t, conn.Out(), "0")
		}
	})
	t.Run("by score rev", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 10)
		_, _ = red.ZSet().Add("key", "two", 20)
		_, _ = red.ZSet().Add("key", "thr", 30)
		_, _ = red.ZSet().Add("key", "2nd", 20)

		{
			cmd := redis.MustParse(ParseZRange, "zrange key 0 10 byscore rev")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 1)
			be.Equal(t, conn.Out(), "1,one")
		}
		{
			cmd := redis.MustParse(ParseZRange, "zrange key 0 50 byscore rev")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 4)
			be.Equal(t, conn.Out(), "4,thr,two,2nd,one")
		}
		{
			cmd := redis.MustParse(ParseZRange, "zrange key 30 50 byscore rev")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 1)
			be.Equal(t, conn.Out(), "1,thr")
		}
		{
			cmd := redis.MustParse(ParseZRange, "zrange key 40 50 byscore rev")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 0)
			be.Equal(t, conn.Out(), "0")
		}
	})
	t.Run("limit", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 10)
		_, _ = red.ZSet().Add("key", "two", 20)
		_, _ = red.ZSet().Add("key", "thr", 30)
		_, _ = red.ZSet().Add("key", "2nd", 20)

		{
			cmd := redis.MustParse(ParseZRange, "zrange key 0 50 byscore limit 0 2")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 2)
			be.Equal(t, conn.Out(), "2,one,2nd")
		}
		{
			cmd := redis.MustParse(ParseZRange, "zrange key 0 50 byscore limit 1 2")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 2)
			be.Equal(t, conn.Out(), "2,2nd,two")
		}
		{
			cmd := redis.MustParse(ParseZRange, "zrange key 0 50 byscore limit 2 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 2)
			be.Equal(t, conn.Out(), "2,two,thr")
		}
		{
			cmd := redis.MustParse(ParseZRange, "zrange key 0 50 byscore limit 1 -1")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, len(res.([]rzset.SetItem)), 3)
			be.Equal(t, conn.Out(), "3,2nd,two,thr")
		}
	})
	t.Run("with scores", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 1)
		_, _ = red.ZSet().Add("key", "two", 2)
		_, _ = red.ZSet().Add("key", "thr", 3)
		_, _ = red.ZSet().Add("key", "2nd", 2)

		cmd := redis.MustParse(ParseZRange, "zrange key 0 5 withscores")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]rzset.SetItem)), 4)
		be.Equal(t, conn.Out(), "8,one,1,2nd,2,two,2,thr,3")
	})
	t.Run("negative indexes", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 1)
		_, _ = red.ZSet().Add("key", "two", 2)
		_, _ = red.ZSet().Add("key", "thr", 3)
		_, _ = red.ZSet().Add("key", "2nd", 2)

		cmd := redis.MustParse(ParseZRange, "zrange key -2 -1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]rzset.SetItem)), 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseZRange, "zrange key 0 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]rzset.SetItem)), 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "value")

		cmd := redis.MustParse(ParseZRange, "zrange key 0 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]rzset.SetItem)), 0)
		be.Equal(t, conn.Out(), "0")
	})
}
