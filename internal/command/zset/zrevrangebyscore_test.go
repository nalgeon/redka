package zset

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRevRangeByScoreParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZRevRangeByScore
		err  error
	}{
		{
			cmd:  "zrevrangebyscore",
			want: ZRevRangeByScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrevrangebyscore key",
			want: ZRevRangeByScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrevrangebyscore key 11",
			want: ZRevRangeByScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrevrangebyscore key 11 22",
			want: ZRevRangeByScore{key: "key", min: 11.0, max: 22.0},
			err:  nil,
		},
		{
			cmd:  "zrevrangebyscore key (1 (2",
			want: ZRevRangeByScore{},
			err:  redis.ErrInvalidFloat,
		},
		{
			cmd:  "zrevrangebyscore key 11 22 limit 10",
			want: ZRevRangeByScore{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd:  "zrevrangebyscore key 11 22 limit 10 5",
			want: ZRevRangeByScore{key: "key", min: 11.0, max: 22.0, offset: 10, count: 5},
			err:  nil,
		},
		{
			cmd:  "zrevrangebyscore key 11 22 withscores",
			want: ZRevRangeByScore{key: "key", min: 11.0, max: 22.0, withScores: true},
			err:  nil,
		},
		{
			cmd: "zrevrangebyscore key 11 22 limit 10 5 withscores",
			want: ZRevRangeByScore{
				key: "key", min: 11.0, max: 22.0,
				offset: 10, count: 5,
				withScores: true,
			},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZRevRangeByScore, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.min, test.want.min)
				testx.AssertEqual(t, cmd.max, test.want.max)
				testx.AssertEqual(t, cmd.offset, test.want.offset)
				testx.AssertEqual(t, cmd.count, test.want.count)
				testx.AssertEqual(t, cmd.withScores, test.want.withScores)
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
			cmd := redis.MustParse(ParseZRevRangeByScore, "zrevrangebyscore key 0 10")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.Out(), "1,one")
		}
		{
			cmd := redis.MustParse(ParseZRevRangeByScore, "zrevrangebyscore key 0 50")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
			testx.AssertEqual(t, conn.Out(), "4,thr,two,2nd,one")
		}
		{
			cmd := redis.MustParse(ParseZRevRangeByScore, "zrevrangebyscore key 30 50")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.Out(), "1,thr")
		}
		{
			cmd := redis.MustParse(ParseZRevRangeByScore, "zrevrangebyscore key 40 50")
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
			cmd := redis.MustParse(ParseZRevRangeByScore, "zrevrangebyscore key 0 50 limit 0 2")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.Out(), "2,thr,two")
		}
		{
			cmd := redis.MustParse(ParseZRevRangeByScore, "zrevrangebyscore key 0 50 limit 1 2")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.Out(), "2,two,2nd")
		}
		{
			cmd := redis.MustParse(ParseZRevRangeByScore, "zrevrangebyscore key 0 50 limit 2 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.Out(), "2,2nd,one")
		}
		{
			cmd := redis.MustParse(ParseZRevRangeByScore, "zrevrangebyscore key 0 50 limit 1 -1")
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

		cmd := redis.MustParse(ParseZRevRangeByScore, "zrevrangebyscore key 10 50 withscores")
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

		cmd := redis.MustParse(ParseZRevRangeByScore, "zrevrangebyscore key -20 -10")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 3)
		testx.AssertEqual(t, conn.Out(), "3,one,two,2nd")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseZRevRangeByScore, "zrevrangebyscore key 0 1")
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

		cmd := redis.MustParse(ParseZRevRangeByScore, "zrevrangebyscore key 0 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
