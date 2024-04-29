package zset_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/zset"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRevRangeParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want zset.ZRevRange
		err  error
	}{
		{
			name: "zrevrange",
			args: command.BuildArgs("zrevrange"),
			want: zset.ZRevRange{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zrevrange key",
			args: command.BuildArgs("zrevrange", "key"),
			want: zset.ZRevRange{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zrevrange key 11",
			args: command.BuildArgs("zrevrange", "key", "11"),
			want: zset.ZRevRange{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zrevrange key 11 22",
			args: command.BuildArgs("zrevrange", "key", "11", "22"),
			want: zset.ZRevRange{Key: "key", Start: 11, Stop: 22},
			err:  nil,
		},
		{
			name: "zrevrange key 11 22 withscores",
			args: command.BuildArgs("zrevrange", "key", "11", "22", "withscores"),
			want: zset.ZRevRange{Key: "key", Start: 11, Stop: 22, WithScores: true},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*zset.ZRevRange)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Start, test.want.Start)
				testx.AssertEqual(t, cm.Stop, test.want.Stop)
				testx.AssertEqual(t, cm.WithScores, test.want.WithScores)
			}
		})
	}
}

func TestZRevRangeExec(t *testing.T) {
	t.Run("range", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)
		_, _ = db.ZSet().Add("key", "2nd", 2)

		{
			cmd := command.MustParse[*zset.ZRevRange]("zrevrange key 0 1")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.Out(), "2,thr,two")
		}
		{
			cmd := command.MustParse[*zset.ZRevRange]("zrevrange key 0 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
			testx.AssertEqual(t, conn.Out(), "4,thr,two,2nd,one")
		}
		{
			cmd := command.MustParse[*zset.ZRevRange]("zrevrange key 3 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.Out(), "1,one")
		}
		{
			cmd := command.MustParse[*zset.ZRevRange]("zrevrange key 4 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
			testx.AssertEqual(t, conn.Out(), "0")
		}
	})
	t.Run("with scores", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)
		_, _ = db.ZSet().Add("key", "2nd", 2)

		cmd := command.MustParse[*zset.ZRevRange]("zrevrange key 0 5 withscores")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
		testx.AssertEqual(t, conn.Out(), "8,thr,3,two,2,2nd,2,one,1")
	})
	t.Run("negative indexes", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)
		_, _ = db.ZSet().Add("key", "2nd", 2)

		cmd := command.MustParse[*zset.ZRevRange]("zrevrange key -2 -1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*zset.ZRevRange]("zrevrange key 0 1")
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

		cmd := command.MustParse[*zset.ZRevRange]("zrevrange key 0 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
