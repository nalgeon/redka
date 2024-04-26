package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRevRangeParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZRevRange
		err  error
	}{
		{
			name: "zrevrange",
			args: buildArgs("zrevrange"),
			want: ZRevRange{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrevrange key",
			args: buildArgs("zrevrange", "key"),
			want: ZRevRange{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrevrange key 11",
			args: buildArgs("zrevrange", "key", "11"),
			want: ZRevRange{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrevrange key 11 22",
			args: buildArgs("zrevrange", "key", "11", "22"),
			want: ZRevRange{key: "key", start: 11, stop: 22},
			err:  nil,
		},
		{
			name: "zrevrange key 11 22 withscores",
			args: buildArgs("zrevrange", "key", "11", "22", "withscores"),
			want: ZRevRange{key: "key", start: 11, stop: 22, withScores: true},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZRevRange)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.start, test.want.start)
				testx.AssertEqual(t, cm.stop, test.want.stop)
				testx.AssertEqual(t, cm.withScores, test.want.withScores)
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
			cmd := mustParse[*ZRevRange]("zrevrange key 0 1")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
			testx.AssertEqual(t, conn.out(), "2,thr,two")
		}
		{
			cmd := mustParse[*ZRevRange]("zrevrange key 0 5")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
			testx.AssertEqual(t, conn.out(), "4,thr,two,2nd,one")
		}
		{
			cmd := mustParse[*ZRevRange]("zrevrange key 3 5")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
			testx.AssertEqual(t, conn.out(), "1,one")
		}
		{
			cmd := mustParse[*ZRevRange]("zrevrange key 4 5")
			conn := new(fakeConn)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
			testx.AssertEqual(t, conn.out(), "0")
		}
	})
	t.Run("with scores", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)
		_, _ = db.ZSet().Add("key", "2nd", 2)

		cmd := mustParse[*ZRevRange]("zrevrange key 0 5 withscores")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
		testx.AssertEqual(t, conn.out(), "8,thr,3,two,2,2nd,2,one,1")
	})
	t.Run("negative indexes", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)
		_, _ = db.ZSet().Add("key", "2nd", 2)

		cmd := mustParse[*ZRevRange]("zrevrange key -2 -1")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*ZRevRange]("zrevrange key 0 1")
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

		cmd := mustParse[*ZRevRange]("zrevrange key 0 1")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
}
