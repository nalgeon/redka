package zset_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/zset"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRemRangeByScoreParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want zset.ZRemRangeByScore
		err  error
	}{
		{
			name: "zremrangebyscore",
			args: command.BuildArgs("zremrangebyscore"),
			want: zset.ZRemRangeByScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zremrangebyscore key",
			args: command.BuildArgs("zremrangebyscore", "key"),
			want: zset.ZRemRangeByScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zremrangebyscore key 1.1",
			args: command.BuildArgs("zremrangebyscore", "key", "1.1"),
			want: zset.ZRemRangeByScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zremrangebyscore key 1.1 2.2",
			args: command.BuildArgs("zremrangebyscore", "key", "1.1", "2.2"),
			want: zset.ZRemRangeByScore{Key: "key", Min: 1.1, Max: 2.2},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*zset.ZRemRangeByScore)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Min, test.want.Min)
				testx.AssertEqual(t, cm.Max, test.want.Max)
			}
		})
	}
}

func TestZRemRangeByScoreExec(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 10)
		_, _ = db.ZSet().Add("key", "two", 20)
		_, _ = db.ZSet().Add("key", "2nd", 20)
		_, _ = db.ZSet().Add("key", "thr", 30)

		cmd := command.MustParse[*zset.ZRemRangeByScore]("zremrangebyscore key 10 20")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 3)
		testx.AssertEqual(t, conn.Out(), "3")

		count, _ := db.ZSet().Len("key")
		testx.AssertEqual(t, count, 1)

		_, err = db.ZSet().GetScore("key", "one")
		testx.AssertErr(t, err, core.ErrNotFound)
		_, err = db.ZSet().GetScore("key", "two")
		testx.AssertErr(t, err, core.ErrNotFound)
		_, err = db.ZSet().GetScore("key", "2nd")
		testx.AssertErr(t, err, core.ErrNotFound)
		thr, _ := db.ZSet().GetScore("key", "thr")
		testx.AssertEqual(t, thr, 30.0)
	})
	t.Run("all", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 10)
		_, _ = db.ZSet().Add("key", "two", 20)
		_, _ = db.ZSet().Add("key", "2nd", 20)
		_, _ = db.ZSet().Add("key", "thr", 30)

		cmd := command.MustParse[*zset.ZRemRangeByScore]("zremrangebyscore key 0 30")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 4)
		testx.AssertEqual(t, conn.Out(), "4")

		count, _ := db.ZSet().Len("key")
		testx.AssertEqual(t, count, 0)
	})
	t.Run("none", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 10)
		_, _ = db.ZSet().Add("key", "two", 20)
		_, _ = db.ZSet().Add("key", "2nd", 20)
		_, _ = db.ZSet().Add("key", "thr", 30)

		cmd := command.MustParse[*zset.ZRemRangeByScore]("zremrangebyscore key 40 50")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		count, _ := db.ZSet().Len("key")
		testx.AssertEqual(t, count, 4)
	})
	t.Run("negative scores", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", -10)
		_, _ = db.ZSet().Add("key", "two", -20)
		_, _ = db.ZSet().Add("key", "2nd", -20)
		_, _ = db.ZSet().Add("key", "thr", -30)

		cmd := command.MustParse[*zset.ZRemRangeByScore]("zremrangebyscore key -20 -10")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 3)
		testx.AssertEqual(t, conn.Out(), "3")

		count, _ := db.ZSet().Len("key")
		testx.AssertEqual(t, count, 1)
		thr, _ := db.ZSet().GetScore("key", "thr")
		testx.AssertEqual(t, thr, -30.0)
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*zset.ZRemRangeByScore]("zremrangebyscore key 0 30")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = red.Str().Set("key", "str")

		cmd := command.MustParse[*zset.ZRemRangeByScore]("zremrangebyscore key 0 30")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		val, _ := red.Str().Get("key")
		testx.AssertEqual(t, val.String(), "str")
	})
}
