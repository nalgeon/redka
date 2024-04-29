package zset_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/zset"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRevRangeByScoreParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want zset.ZRevRangeByScore
		err  error
	}{
		{
			name: "zrevrangebyscore",
			args: command.BuildArgs("zrevrangebyscore"),
			want: zset.ZRevRangeByScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zrevrangebyscore key",
			args: command.BuildArgs("zrevrangebyscore", "key"),
			want: zset.ZRevRangeByScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zrevrangebyscore key 11",
			args: command.BuildArgs("zrevrangebyscore", "key", "11"),
			want: zset.ZRevRangeByScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zrevrangebyscore key 11 22",
			args: command.BuildArgs("zrevrangebyscore", "key", "11", "22"),
			want: zset.ZRevRangeByScore{Key: "key", Min: 11.0, Max: 22.0},
			err:  nil,
		},
		{
			name: "zrevrangebyscore key exclusive",
			args: command.BuildArgs("zrevrangebyscore", "key", "(1", "(2"),
			want: zset.ZRevRangeByScore{},
			err:  redis.ErrInvalidFloat,
		},
		{
			name: "zrevrangebyscore key 11 22 limit 10",
			args: command.BuildArgs("zrevrangebyscore", "key", "11", "22", "limit", "10"),
			want: zset.ZRevRangeByScore{},
			err:  redis.ErrSyntaxError,
		},
		{
			name: "zrevrangebyscore key 11 22 limit 10 5",
			args: command.BuildArgs("zrevrangebyscore", "key", "11", "22", "limit", "10", "5"),
			want: zset.ZRevRangeByScore{Key: "key", Min: 11.0, Max: 22.0, Offset: 10, Count: 5},
			err:  nil,
		},
		{
			name: "zrevrangebyscore key 11 22 withscores",
			args: command.BuildArgs("zrevrangebyscore", "key", "11", "22", "withscores"),
			want: zset.ZRevRangeByScore{Key: "key", Min: 11.0, Max: 22.0, WithScores: true},
			err:  nil,
		},
		{
			name: "zrevrangebyscore key 11 22 limit 10 5 withscores",
			args: command.BuildArgs("zrevrangebyscore", "key", "11", "22",
				"limit", "10", "5", "withscores"),
			want: zset.ZRevRangeByScore{Key: "key", Min: 11.0, Max: 22.0,
				Offset: 10, Count: 5, WithScores: true},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*zset.ZRevRangeByScore)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Min, test.want.Min)
				testx.AssertEqual(t, cm.Max, test.want.Max)
				testx.AssertEqual(t, cm.Offset, test.want.Offset)
				testx.AssertEqual(t, cm.Count, test.want.Count)
				testx.AssertEqual(t, cm.WithScores, test.want.WithScores)
			}
		})
	}
}

func TestZRevRangeByScoreExec(t *testing.T) {
	t.Run("range", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 10)
		_, _ = db.ZSet().Add("key", "two", 20)
		_, _ = db.ZSet().Add("key", "thr", 30)
		_, _ = db.ZSet().Add("key", "2nd", 20)

		{
			cmd := command.MustParse[*zset.ZRevRangeByScore]("zrevrangebyscore key 0 10")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.Out(), "1,one")
		}
		{
			cmd := command.MustParse[*zset.ZRevRangeByScore]("zrevrangebyscore key 0 50")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
			testx.AssertEqual(t, conn.Out(), "4,thr,two,2nd,one")
		}
		{
			cmd := command.MustParse[*zset.ZRevRangeByScore]("zrevrangebyscore key 30 50")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.Out(), "1,thr")
		}
		{
			cmd := command.MustParse[*zset.ZRevRangeByScore]("zrevrangebyscore key 40 50")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
			testx.AssertEqual(t, conn.Out(), "0")
		}
	})
	t.Run("limit", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 10)
		_, _ = db.ZSet().Add("key", "two", 20)
		_, _ = db.ZSet().Add("key", "thr", 30)
		_, _ = db.ZSet().Add("key", "2nd", 20)

		{
			cmd := command.MustParse[*zset.ZRevRangeByScore]("zrevrangebyscore key 0 50 limit 0 2")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.Out(), "2,thr,two")
		}
		{
			cmd := command.MustParse[*zset.ZRevRangeByScore]("zrevrangebyscore key 0 50 limit 1 2")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.Out(), "2,two,2nd")
		}
		{
			cmd := command.MustParse[*zset.ZRevRangeByScore]("zrevrangebyscore key 0 50 limit 2 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.Out(), "2,2nd,one")
		}
		{
			cmd := command.MustParse[*zset.ZRevRangeByScore]("zrevrangebyscore key 0 50 limit 1 -1")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 3)
			testx.AssertEqual(t, conn.Out(), "3,two,2nd,one")
		}
	})
	t.Run("with scores", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 10)
		_, _ = db.ZSet().Add("key", "two", 20)
		_, _ = db.ZSet().Add("key", "thr", 30)
		_, _ = db.ZSet().Add("key", "2nd", 20)

		cmd := command.MustParse[*zset.ZRevRangeByScore]("zrevrangebyscore key 10 50 withscores")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
		testx.AssertEqual(t, conn.Out(), "8,thr,30,two,20,2nd,20,one,10")
	})
	t.Run("negative indexes", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", -10)
		_, _ = db.ZSet().Add("key", "two", -20)
		_, _ = db.ZSet().Add("key", "thr", -30)
		_, _ = db.ZSet().Add("key", "2nd", -20)

		cmd := command.MustParse[*zset.ZRevRangeByScore]("zrevrangebyscore key -20 -10")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 3)
		testx.AssertEqual(t, conn.Out(), "3,one,two,2nd")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*zset.ZRevRangeByScore]("zrevrangebyscore key 0 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := command.MustParse[*zset.ZRevRangeByScore]("zrevrangebyscore key 0 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
