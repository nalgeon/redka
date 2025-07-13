package zset

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestZRemRangeByScoreParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZRemRangeByScore
		err  error
	}{
		{
			cmd:  "zremrangebyscore",
			want: ZRemRangeByScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zremrangebyscore key",
			want: ZRemRangeByScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zremrangebyscore key 1.1",
			want: ZRemRangeByScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zremrangebyscore key 1.1 2.2",
			want: ZRemRangeByScore{key: "key", min: 1.1, max: 2.2},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZRemRangeByScore, test.cmd)
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

func TestZRemRangeByScoreExec(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 10)
		_, _ = red.ZSet().Add("key", "two", 20)
		_, _ = red.ZSet().Add("key", "2nd", 20)
		_, _ = red.ZSet().Add("key", "thr", 30)

		cmd := redis.MustParse(ParseZRemRangeByScore, "zremrangebyscore key 10 20")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 3)
		be.Equal(t, conn.Out(), "3")

		count, _ := red.ZSet().Len("key")
		be.Equal(t, count, 1)

		_, err = red.ZSet().GetScore("key", "one")
		be.Err(t, err, core.ErrNotFound)
		_, err = red.ZSet().GetScore("key", "two")
		be.Err(t, err, core.ErrNotFound)
		_, err = red.ZSet().GetScore("key", "2nd")
		be.Err(t, err, core.ErrNotFound)
		thr, _ := red.ZSet().GetScore("key", "thr")
		be.Equal(t, thr, 30.0)
	})
	t.Run("all", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 10)
		_, _ = red.ZSet().Add("key", "two", 20)
		_, _ = red.ZSet().Add("key", "2nd", 20)
		_, _ = red.ZSet().Add("key", "thr", 30)

		cmd := redis.MustParse(ParseZRemRangeByScore, "zremrangebyscore key 0 30")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 4)
		be.Equal(t, conn.Out(), "4")

		count, _ := red.ZSet().Len("key")
		be.Equal(t, count, 0)
	})
	t.Run("none", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 10)
		_, _ = red.ZSet().Add("key", "two", 20)
		_, _ = red.ZSet().Add("key", "2nd", 20)
		_, _ = red.ZSet().Add("key", "thr", 30)

		cmd := redis.MustParse(ParseZRemRangeByScore, "zremrangebyscore key 40 50")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		count, _ := red.ZSet().Len("key")
		be.Equal(t, count, 4)
	})
	t.Run("negative scores", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", -10)
		_, _ = red.ZSet().Add("key", "two", -20)
		_, _ = red.ZSet().Add("key", "2nd", -20)
		_, _ = red.ZSet().Add("key", "thr", -30)

		cmd := redis.MustParse(ParseZRemRangeByScore, "zremrangebyscore key -20 -10")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 3)
		be.Equal(t, conn.Out(), "3")

		count, _ := red.ZSet().Len("key")
		be.Equal(t, count, 1)
		thr, _ := red.ZSet().GetScore("key", "thr")
		be.Equal(t, thr, -30.0)
	})
	t.Run("key not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseZRemRangeByScore, "zremrangebyscore key 0 30")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "str")

		cmd := redis.MustParse(ParseZRemRangeByScore, "zremrangebyscore key 0 30")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		val, _ := red.Str().Get("key")
		be.Equal(t, val.String(), "str")
	})
}
