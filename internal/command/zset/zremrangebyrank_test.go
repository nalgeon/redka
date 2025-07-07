package zset

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

func TestZRemRangeByRankParse(t *testing.T) {
	tests := []struct {
		cmd  string
		args [][]byte
		want ZRemRangeByRank
		err  error
	}{
		{
			cmd:  "zremrangebyrank",
			want: ZRemRangeByRank{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zremrangebyrank key",
			want: ZRemRangeByRank{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zremrangebyrank key 11",
			want: ZRemRangeByRank{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zremrangebyrank key 11 22",
			want: ZRemRangeByRank{key: "key", start: 11, stop: 22},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZRemRangeByRank, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.start, test.want.start)
				be.Equal(t, cmd.stop, test.want.stop)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestZRemRangeByRankExec(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "2nd", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)

		cmd := redis.MustParse(ParseZRemRangeByRank, "zremrangebyrank key 1 2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		count, _ := db.ZSet().Len("key")
		be.Equal(t, count, 2)

		one, _ := db.ZSet().GetScore("key", "one")
		be.Equal(t, one, 1.0)
		_, err = db.ZSet().GetScore("key", "two")
		be.Err(t, err, core.ErrNotFound)
		_, err = db.ZSet().GetScore("key", "2nd")
		be.Err(t, err, core.ErrNotFound)
		thr, _ := db.ZSet().GetScore("key", "thr")
		be.Equal(t, thr, 3.0)
	})
	t.Run("all", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "2nd", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)

		cmd := redis.MustParse(ParseZRemRangeByRank, "zremrangebyrank key 0 3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 4)
		be.Equal(t, conn.Out(), "4")

		count, _ := db.ZSet().Len("key")
		be.Equal(t, count, 0)
	})
	t.Run("none", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "2nd", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)

		cmd := redis.MustParse(ParseZRemRangeByRank, "zremrangebyrank key 4 5")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		count, _ := db.ZSet().Len("key")
		be.Equal(t, count, 4)
	})
	t.Run("negative indexes", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "2nd", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)

		cmd := redis.MustParse(ParseZRemRangeByRank, "zremrangebyrank key -2 -1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		count, _ := db.ZSet().Len("key")
		be.Equal(t, count, 4)
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseZRemRangeByRank, "zremrangebyrank key 0 3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = red.Str().Set("key", "str")

		cmd := redis.MustParse(ParseZRemRangeByRank, "zremrangebyrank key 0 3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		val, _ := red.Str().Get("key")
		be.Equal(t, val.String(), "str")
	})
}
