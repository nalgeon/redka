package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRevRangeByScoreParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZRevRangeByScore
		err  error
	}{
		{
			name: "zrevrangebyscore",
			args: buildArgs("zrevrangebyscore"),
			want: ZRevRangeByScore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrevrangebyscore key",
			args: buildArgs("zrevrangebyscore", "key"),
			want: ZRevRangeByScore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrevrangebyscore key 11",
			args: buildArgs("zrevrangebyscore", "key", "11"),
			want: ZRevRangeByScore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrevrangebyscore key 11 22",
			args: buildArgs("zrevrangebyscore", "key", "11", "22"),
			want: ZRevRangeByScore{key: "key", min: 11.0, max: 22.0},
			err:  nil,
		},
		{
			name: "zrevrangebyscore key exclusive",
			args: buildArgs("zrevrangebyscore", "key", "(1", "(2"),
			want: ZRevRangeByScore{},
			err:  ErrInvalidFloat,
		},
		{
			name: "zrevrangebyscore key 11 22 limit 10",
			args: buildArgs("zrevrangebyscore", "key", "11", "22", "limit", "10"),
			want: ZRevRangeByScore{},
			err:  ErrSyntaxError,
		},
		{
			name: "zrevrangebyscore key 11 22 limit 10 5",
			args: buildArgs("zrevrangebyscore", "key", "11", "22", "limit", "10", "5"),
			want: ZRevRangeByScore{key: "key", min: 11.0, max: 22.0, offset: 10, count: 5},
			err:  nil,
		},
		{
			name: "zrevrangebyscore key 11 22 withscores",
			args: buildArgs("zrevrangebyscore", "key", "11", "22", "withscores"),
			want: ZRevRangeByScore{key: "key", min: 11.0, max: 22.0, withScores: true},
			err:  nil,
		},
		{
			name: "zrevrangebyscore key 11 22 limit 10 5 withscores",
			args: buildArgs("zrevrangebyscore", "key", "11", "22",
				"limit", "10", "5", "withscores"),
			want: ZRevRangeByScore{key: "key", min: 11.0, max: 22.0,
				offset: 10, count: 5, withScores: true},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZRevRangeByScore)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.min, test.want.min)
				testx.AssertEqual(t, cm.max, test.want.max)
				testx.AssertEqual(t, cm.offset, test.want.offset)
				testx.AssertEqual(t, cm.count, test.want.count)
				testx.AssertEqual(t, cm.withScores, test.want.withScores)
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
			cmd := mustParse[*ZRevRangeByScore]("zrevrangebyscore key 0 10")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.out(), "1,one")
		}
		{
			cmd := mustParse[*ZRevRangeByScore]("zrevrangebyscore key 0 50")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
			testx.AssertEqual(t, conn.out(), "4,thr,two,2nd,one")
		}
		{
			cmd := mustParse[*ZRevRangeByScore]("zrevrangebyscore key 30 50")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.out(), "1,thr")
		}
		{
			cmd := mustParse[*ZRevRangeByScore]("zrevrangebyscore key 40 50")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
			testx.AssertEqual(t, conn.out(), "0")
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
			cmd := mustParse[*ZRevRangeByScore]("zrevrangebyscore key 0 50 limit 0 2")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.out(), "2,thr,two")
		}
		{
			cmd := mustParse[*ZRevRangeByScore]("zrevrangebyscore key 0 50 limit 1 2")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.out(), "2,two,2nd")
		}
		{
			cmd := mustParse[*ZRevRangeByScore]("zrevrangebyscore key 0 50 limit 2 5")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.out(), "2,2nd,one")
		}
		{
			cmd := mustParse[*ZRevRangeByScore]("zrevrangebyscore key 0 50 limit 1 -1")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 3)
			testx.AssertEqual(t, conn.out(), "3,two,2nd,one")
		}
	})
	t.Run("with scores", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 10)
		_, _ = db.ZSet().Add("key", "two", 20)
		_, _ = db.ZSet().Add("key", "thr", 30)
		_, _ = db.ZSet().Add("key", "2nd", 20)

		cmd := mustParse[*ZRevRangeByScore]("zrevrangebyscore key 10 50 withscores")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
		testx.AssertEqual(t, conn.out(), "8,thr,30,two,20,2nd,20,one,10")
	})
	t.Run("negative indexes", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", -10)
		_, _ = db.ZSet().Add("key", "two", -20)
		_, _ = db.ZSet().Add("key", "thr", -30)
		_, _ = db.ZSet().Add("key", "2nd", -20)

		cmd := mustParse[*ZRevRangeByScore]("zrevrangebyscore key -20 -10")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 3)
		testx.AssertEqual(t, conn.out(), "3,one,two,2nd")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*ZRevRangeByScore]("zrevrangebyscore key 0 1")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := mustParse[*ZRevRangeByScore]("zrevrangebyscore key 0 1")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
}
