package zset_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/zset"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRangeParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want zset.ZRange
		err  error
	}{
		{
			name: "zrange",
			args: command.BuildArgs("zrange"),
			want: zset.ZRange{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zrange key",
			args: command.BuildArgs("zrange", "key"),
			want: zset.ZRange{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zrange key 11",
			args: command.BuildArgs("zrange", "key", "11"),
			want: zset.ZRange{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zrange key 11 22",
			args: command.BuildArgs("zrange", "key", "11", "22"),
			want: zset.ZRange{Key: "key", Start: 11.0, Stop: 22.0},
			err:  nil,
		},
		{
			name: "zrange key 1.1 2.2 byscore",
			args: command.BuildArgs("zrange", "key", "1.1", "2.2", "byscore"),
			want: zset.ZRange{Key: "key", Start: 1.1, Stop: 2.2, ByScore: true},
			err:  nil,
		},
		{
			name: "zrange key byscore exclusive",
			args: command.BuildArgs("zrange", "key", "(1", "(2", "byscore"),
			want: zset.ZRange{},
			err:  redis.ErrInvalidFloat,
		},
		{
			name: "zrange key 11 22 rev",
			args: command.BuildArgs("zrange", "key", "11", "22", "rev"),
			want: zset.ZRange{Key: "key", Start: 11.0, Stop: 22.0, Rev: true},
			err:  nil,
		},
		{
			name: "zrange key 11 22 byscore limit 10",
			args: command.BuildArgs("zrange", "key", "11", "22", "byscore", "limit", "10"),
			want: zset.ZRange{},
			err:  redis.ErrSyntaxError,
		},
		{
			name: "zrange key 11 22 byscore limit 10 5",
			args: command.BuildArgs("zrange", "key", "11", "22", "byscore", "limit", "10", "5"),
			want: zset.ZRange{Key: "key", Start: 11.0, Stop: 22.0, ByScore: true, Offset: 10, Count: 5},
			err:  nil,
		},
		{
			name: "zrange key 11 22 withscores",
			args: command.BuildArgs("zrange", "key", "11", "22", "withscores"),
			want: zset.ZRange{Key: "key", Start: 11.0, Stop: 22.0, WithScores: true},
			err:  nil,
		},
		{
			name: "zrange key 11 22 limit 10 5 rev byscore withscores",
			args: command.BuildArgs("zrange", "key", "11", "22", "limit", "10", "5",
				"rev", "byscore", "withscores"),
			want: zset.ZRange{Key: "key", Start: 11.0, Stop: 22.0, ByScore: true,
				Rev: true, Offset: 10, Count: 5, WithScores: true},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*zset.ZRange)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Start, test.want.Start)
				testx.AssertEqual(t, cm.Stop, test.want.Stop)
				testx.AssertEqual(t, cm.ByScore, test.want.ByScore)
				testx.AssertEqual(t, cm.Rev, test.want.Rev)
				testx.AssertEqual(t, cm.Offset, test.want.Offset)
				testx.AssertEqual(t, cm.Count, test.want.Count)
				testx.AssertEqual(t, cm.WithScores, test.want.WithScores)
			}
		})
	}
}

func TestZRangeExec(t *testing.T) {
	t.Run("by rank", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)
		_, _ = db.ZSet().Add("key", "2nd", 2)

		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 0 1")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.Out(), "2,one,2nd")
		}
		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 0 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
			testx.AssertEqual(t, conn.Out(), "4,one,2nd,two,thr")
		}
		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 3 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.Out(), "1,thr")
		}
		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 4 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
			testx.AssertEqual(t, conn.Out(), "0")
		}
	})
	t.Run("by rank rev", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)
		_, _ = db.ZSet().Add("key", "2nd", 2)

		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 0 1 rev")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.Out(), "2,thr,two")
		}
		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 0 5 rev")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
			testx.AssertEqual(t, conn.Out(), "4,thr,two,2nd,one")
		}
		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 3 5 rev")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.Out(), "1,one")
		}
		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 4 5 rev")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
			testx.AssertEqual(t, conn.Out(), "0")
		}
	})
	t.Run("by score", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 10)
		_, _ = db.ZSet().Add("key", "two", 20)
		_, _ = db.ZSet().Add("key", "thr", 30)
		_, _ = db.ZSet().Add("key", "2nd", 20)

		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 0 10 byscore")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.Out(), "1,one")
		}
		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 0 50 byscore")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
			testx.AssertEqual(t, conn.Out(), "4,one,2nd,two,thr")
		}
		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 30 50 byscore")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.Out(), "1,thr")
		}
		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 40 50 byscore")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
			testx.AssertEqual(t, conn.Out(), "0")
		}
	})
	t.Run("by score rev", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 10)
		_, _ = db.ZSet().Add("key", "two", 20)
		_, _ = db.ZSet().Add("key", "thr", 30)
		_, _ = db.ZSet().Add("key", "2nd", 20)

		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 0 10 byscore rev")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.Out(), "1,one")
		}
		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 0 50 byscore rev")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
			testx.AssertEqual(t, conn.Out(), "4,thr,two,2nd,one")
		}
		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 30 50 byscore rev")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.Out(), "1,thr")
		}
		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 40 50 byscore rev")
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
			cmd := command.MustParse[*zset.ZRange]("zrange key 0 50 byscore limit 0 2")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.Out(), "2,one,2nd")
		}
		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 0 50 byscore limit 1 2")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.Out(), "2,2nd,two")
		}
		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 0 50 byscore limit 2 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.Out(), "2,two,thr")
		}
		{
			cmd := command.MustParse[*zset.ZRange]("zrange key 0 50 byscore limit 1 -1")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 3)
			testx.AssertEqual(t, conn.Out(), "3,2nd,two,thr")
		}
	})
	t.Run("with scores", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)
		_, _ = db.ZSet().Add("key", "2nd", 2)

		cmd := command.MustParse[*zset.ZRange]("zrange key 0 5 withscores")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
		testx.AssertEqual(t, conn.Out(), "8,one,1,2nd,2,two,2,thr,3")
	})
	t.Run("negative indexes", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)
		_, _ = db.ZSet().Add("key", "2nd", 2)

		cmd := command.MustParse[*zset.ZRange]("zrange key -2 -1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*zset.ZRange]("zrange key 0 1")
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

		cmd := command.MustParse[*zset.ZRange]("zrange key 0 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
