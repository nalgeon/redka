package zset

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRangeByScoreParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZRangeByScore
		err  error
	}{
		{
			cmd:  "zrangebyscore",
			want: ZRangeByScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrangebyscore key",
			want: ZRangeByScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrangebyscore key 11",
			want: ZRangeByScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrangebyscore key 11 22",
			want: ZRangeByScore{key: "key", min: 11.0, max: 22.0},
			err:  nil,
		},
		{
			cmd:  "zrangebyscore key (1 (2",
			want: ZRangeByScore{},
			err:  redis.ErrInvalidFloat,
		},
		{
			cmd:  "zrangebyscore key 11 22 limit 10",
			want: ZRangeByScore{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd:  "zrangebyscore key 11 22 limit 10 5",
			want: ZRangeByScore{key: "key", min: 11.0, max: 22.0, offset: 10, count: 5},
			err:  nil,
		},
		{
			cmd:  "zrangebyscore key 11 22 withscores",
			want: ZRangeByScore{key: "key", min: 11.0, max: 22.0, withScores: true},
			err:  nil,
		},
		{
			cmd: "zrangebyscore key 11 22 limit 10 5 withscores",
			want: ZRangeByScore{
				key: "key", min: 11.0, max: 22.0,
				offset: 10, count: 5,
				withScores: true,
			},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZRangeByScore, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.min, test.want.min)
				testx.AssertEqual(t, cmd.max, test.want.max)
				testx.AssertEqual(t, cmd.offset, test.want.offset)
				testx.AssertEqual(t, cmd.count, test.want.count)
				testx.AssertEqual(t, cmd.withScores, test.want.withScores)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestZRangeByScoreExec(t *testing.T) {
	t.Run("range", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 10)
		_, _ = db.ZSet().Add("key", "two", 20)
		_, _ = db.ZSet().Add("key", "thr", 30)
		_, _ = db.ZSet().Add("key", "2nd", 20)

		{
			cmd := redis.MustParse(ParseZRangeByScore, "zrangebyscore key 0 10")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.Out(), "1,one")
		}
		{
			cmd := redis.MustParse(ParseZRangeByScore, "zrangebyscore key 0 50")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
			testx.AssertEqual(t, conn.Out(), "4,one,2nd,two,thr")
		}
		{
			cmd := redis.MustParse(ParseZRangeByScore, "zrangebyscore key 30 50")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.Out(), "1,thr")
		}
		{
			cmd := redis.MustParse(ParseZRangeByScore, "zrangebyscore key 40 50")
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
			cmd := redis.MustParse(ParseZRangeByScore, "zrangebyscore key 0 50 limit 0 2")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.Out(), "2,one,2nd")
		}
		{
			cmd := redis.MustParse(ParseZRangeByScore, "zrangebyscore key 0 50 limit 1 2")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.Out(), "2,2nd,two")
		}
		{
			cmd := redis.MustParse(ParseZRangeByScore, "zrangebyscore key 0 50 limit 2 5")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.Out(), "2,two,thr")
		}
		{
			cmd := redis.MustParse(ParseZRangeByScore, "zrangebyscore key 0 50 limit 1 -1")
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
		_, _ = db.ZSet().Add("key", "one", 10)
		_, _ = db.ZSet().Add("key", "two", 20)
		_, _ = db.ZSet().Add("key", "thr", 30)
		_, _ = db.ZSet().Add("key", "2nd", 20)

		cmd := redis.MustParse(ParseZRangeByScore, "zrangebyscore key 10 50 withscores")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
		testx.AssertEqual(t, conn.Out(), "8,one,10,2nd,20,two,20,thr,30")
	})
	t.Run("negative indexes", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", -10)
		_, _ = db.ZSet().Add("key", "two", -20)
		_, _ = db.ZSet().Add("key", "thr", -30)
		_, _ = db.ZSet().Add("key", "2nd", -20)

		cmd := redis.MustParse(ParseZRangeByScore, "zrangebyscore key -20 -10")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 3)
		testx.AssertEqual(t, conn.Out(), "3,2nd,two,one")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseZRangeByScore, "zrangebyscore key 0 1")
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

		cmd := redis.MustParse(ParseZRangeByScore, "zrangebyscore key 0 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
