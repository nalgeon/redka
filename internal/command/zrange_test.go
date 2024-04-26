package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRangeParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZRange
		err  error
	}{
		{
			name: "zrange",
			args: buildArgs("zrange"),
			want: ZRange{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrange key",
			args: buildArgs("zrange", "key"),
			want: ZRange{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrange key 11",
			args: buildArgs("zrange", "key", "11"),
			want: ZRange{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrange key 11 22",
			args: buildArgs("zrange", "key", "11", "22"),
			want: ZRange{key: "key", start: 11.0, stop: 22.0},
			err:  nil,
		},
		{
			name: "zrange key 1.1 2.2 byscore",
			args: buildArgs("zrange", "key", "1.1", "2.2", "byscore"),
			want: ZRange{key: "key", start: 1.1, stop: 2.2, byScore: true},
			err:  nil,
		},
		{
			name: "zrange key byscore exclusive",
			args: buildArgs("zrange", "key", "(1", "(2", "byscore"),
			want: ZRange{},
			err:  ErrInvalidFloat,
		},
		{
			name: "zrange key 11 22 rev",
			args: buildArgs("zrange", "key", "11", "22", "rev"),
			want: ZRange{key: "key", start: 11.0, stop: 22.0, rev: true},
			err:  nil,
		},
		{
			name: "zrange key 11 22 byscore limit 10",
			args: buildArgs("zrange", "key", "11", "22", "byscore", "limit", "10"),
			want: ZRange{},
			err:  ErrSyntaxError,
		},
		{
			name: "zrange key 11 22 byscore limit 10 5",
			args: buildArgs("zrange", "key", "11", "22", "byscore", "limit", "10", "5"),
			want: ZRange{key: "key", start: 11.0, stop: 22.0, byScore: true, offset: 10, count: 5},
			err:  nil,
		},
		{
			name: "zrange key 11 22 withscores",
			args: buildArgs("zrange", "key", "11", "22", "withscores"),
			want: ZRange{key: "key", start: 11.0, stop: 22.0, withScores: true},
			err:  nil,
		},
		{
			name: "zrange key 11 22 limit 10 5 rev byscore withscores",
			args: buildArgs("zrange", "key", "11", "22", "limit", "10", "5",
				"rev", "byscore", "withscores"),
			want: ZRange{key: "key", start: 11.0, stop: 22.0, byScore: true,
				rev: true, offset: 10, count: 5, withScores: true},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZRange)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.start, test.want.start)
				testx.AssertEqual(t, cm.stop, test.want.stop)
				testx.AssertEqual(t, cm.byScore, test.want.byScore)
				testx.AssertEqual(t, cm.rev, test.want.rev)
				testx.AssertEqual(t, cm.offset, test.want.offset)
				testx.AssertEqual(t, cm.count, test.want.count)
				testx.AssertEqual(t, cm.withScores, test.want.withScores)
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
			cmd := mustParse[*ZRange]("zrange key 0 1")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.out(), "2,one,2nd")
		}
		{
			cmd := mustParse[*ZRange]("zrange key 0 5")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
			testx.AssertEqual(t, conn.out(), "4,one,2nd,two,thr")
		}
		{
			cmd := mustParse[*ZRange]("zrange key 3 5")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.out(), "1,thr")
		}
		{
			cmd := mustParse[*ZRange]("zrange key 4 5")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
			testx.AssertEqual(t, conn.out(), "0")
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
			cmd := mustParse[*ZRange]("zrange key 0 1 rev")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.out(), "2,thr,two")
		}
		{
			cmd := mustParse[*ZRange]("zrange key 0 5 rev")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
			testx.AssertEqual(t, conn.out(), "4,thr,two,2nd,one")
		}
		{
			cmd := mustParse[*ZRange]("zrange key 3 5 rev")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.out(), "1,one")
		}
		{
			cmd := mustParse[*ZRange]("zrange key 4 5 rev")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
			testx.AssertEqual(t, conn.out(), "0")
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
			cmd := mustParse[*ZRange]("zrange key 0 10 byscore")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.out(), "1,one")
		}
		{
			cmd := mustParse[*ZRange]("zrange key 0 50 byscore")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
			testx.AssertEqual(t, conn.out(), "4,one,2nd,two,thr")
		}
		{
			cmd := mustParse[*ZRange]("zrange key 30 50 byscore")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.out(), "1,thr")
		}
		{
			cmd := mustParse[*ZRange]("zrange key 40 50 byscore")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
			testx.AssertEqual(t, conn.out(), "0")
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
			cmd := mustParse[*ZRange]("zrange key 0 10 byscore rev")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.out(), "1,one")
		}
		{
			cmd := mustParse[*ZRange]("zrange key 0 50 byscore rev")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
			testx.AssertEqual(t, conn.out(), "4,thr,two,2nd,one")
		}
		{
			cmd := mustParse[*ZRange]("zrange key 30 50 byscore rev")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.out(), "1,thr")
		}
		{
			cmd := mustParse[*ZRange]("zrange key 40 50 byscore rev")
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
			cmd := mustParse[*ZRange]("zrange key 0 50 byscore limit 0 2")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.out(), "2,one,2nd")
		}
		{
			cmd := mustParse[*ZRange]("zrange key 0 50 byscore limit 1 2")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.out(), "2,2nd,two")
		}
		{
			cmd := mustParse[*ZRange]("zrange key 0 50 byscore limit 2 5")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.out(), "2,two,thr")
		}
		{
			cmd := mustParse[*ZRange]("zrange key 0 50 byscore limit 1 -1")
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
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)
		_, _ = db.ZSet().Add("key", "2nd", 2)

		cmd := mustParse[*ZRange]("zrange key 0 5 withscores")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
		testx.AssertEqual(t, conn.out(), "8,one,1,2nd,2,two,2,thr,3")
	})
	t.Run("negative indexes", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)
		_, _ = db.ZSet().Add("key", "2nd", 2)

		cmd := mustParse[*ZRange]("zrange key -2 -1")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*ZRange]("zrange key 0 1")
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

		cmd := mustParse[*ZRange]("zrange key 0 1")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
}
