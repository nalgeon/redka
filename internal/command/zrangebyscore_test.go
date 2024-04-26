package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRangeByScoreParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZRangeByScore
		err  error
	}{
		{
			name: "zrangebyscore",
			args: buildArgs("zrangebyscore"),
			want: ZRangeByScore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrangebyscore key",
			args: buildArgs("zrangebyscore", "key"),
			want: ZRangeByScore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrangebyscore key 11",
			args: buildArgs("zrangebyscore", "key", "11"),
			want: ZRangeByScore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrangebyscore key 11 22",
			args: buildArgs("zrangebyscore", "key", "11", "22"),
			want: ZRangeByScore{key: "key", min: 11.0, max: 22.0},
			err:  nil,
		},
		{
			name: "zrangebyscore key exclusive",
			args: buildArgs("zrangebyscore", "key", "(1", "(2"),
			want: ZRangeByScore{},
			err:  ErrInvalidFloat,
		},
		{
			name: "zrangebyscore key 11 22 limit 10",
			args: buildArgs("zrangebyscore", "key", "11", "22", "limit", "10"),
			want: ZRangeByScore{},
			err:  ErrSyntaxError,
		},
		{
			name: "zrangebyscore key 11 22 limit 10 5",
			args: buildArgs("zrangebyscore", "key", "11", "22", "limit", "10", "5"),
			want: ZRangeByScore{key: "key", min: 11.0, max: 22.0, offset: 10, count: 5},
			err:  nil,
		},
		{
			name: "zrangebyscore key 11 22 withscores",
			args: buildArgs("zrangebyscore", "key", "11", "22", "withscores"),
			want: ZRangeByScore{key: "key", min: 11.0, max: 22.0, withScores: true},
			err:  nil,
		},
		{
			name: "zrangebyscore key 11 22 limit 10 5 withscores",
			args: buildArgs("zrangebyscore", "key", "11", "22",
				"limit", "10", "5", "withscores"),
			want: ZRangeByScore{key: "key", min: 11.0, max: 22.0,
				offset: 10, count: 5, withScores: true},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZRangeByScore)
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

func TestZRangeByScoreExec(t *testing.T) {
	t.Run("range", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 10)
		_, _ = db.ZSet().Add("key", "two", 20)
		_, _ = db.ZSet().Add("key", "thr", 30)
		_, _ = db.ZSet().Add("key", "2nd", 20)

		{
			cmd := mustParse[*ZRangeByScore]("zrangebyscore key 0 10")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.out(), "1,one")
		}
		{
			cmd := mustParse[*ZRangeByScore]("zrangebyscore key 0 50")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
			testx.AssertEqual(t, conn.out(), "4,one,2nd,two,thr")
		}
		{
			cmd := mustParse[*ZRangeByScore]("zrangebyscore key 30 50")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.out(), "1,thr")
		}
		{
			cmd := mustParse[*ZRangeByScore]("zrangebyscore key 40 50")
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
			cmd := mustParse[*ZRangeByScore]("zrangebyscore key 0 50 limit 0 2")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.out(), "2,one,2nd")
		}
		{
			cmd := mustParse[*ZRangeByScore]("zrangebyscore key 0 50 limit 1 2")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.out(), "2,2nd,two")
		}
		{
			cmd := mustParse[*ZRangeByScore]("zrangebyscore key 0 50 limit 2 5")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.out(), "2,two,thr")
		}
		{
			cmd := mustParse[*ZRangeByScore]("zrangebyscore key 0 50 limit 1 -1")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 3)
			testx.AssertEqual(t, conn.out(), "3,2nd,two,thr")
		}
	})
	t.Run("with scores", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 10)
		_, _ = db.ZSet().Add("key", "two", 20)
		_, _ = db.ZSet().Add("key", "thr", 30)
		_, _ = db.ZSet().Add("key", "2nd", 20)

		cmd := mustParse[*ZRangeByScore]("zrangebyscore key 10 50 withscores")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
		testx.AssertEqual(t, conn.out(), "8,one,10,2nd,20,two,20,thr,30")
	})
	t.Run("negative indexes", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", -10)
		_, _ = db.ZSet().Add("key", "two", -20)
		_, _ = db.ZSet().Add("key", "thr", -30)
		_, _ = db.ZSet().Add("key", "2nd", -20)

		cmd := mustParse[*ZRangeByScore]("zrangebyscore key -20 -10")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 3)
		testx.AssertEqual(t, conn.out(), "3,2nd,two,one")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*ZRangeByScore]("zrangebyscore key 0 1")
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

		cmd := mustParse[*ZRangeByScore]("zrangebyscore key 0 1")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
}
