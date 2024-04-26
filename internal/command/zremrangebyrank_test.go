package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRemRangeByRankParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZRemRangeByRank
		err  error
	}{
		{
			name: "zremrangebyrank",
			args: buildArgs("zremrangebyrank"),
			want: ZRemRangeByRank{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zremrangebyrank key",
			args: buildArgs("zremrangebyrank", "key"),
			want: ZRemRangeByRank{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zremrangebyrank key 11",
			args: buildArgs("zremrangebyrank", "key", "11"),
			want: ZRemRangeByRank{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zremrangebyrank key 11 22",
			args: buildArgs("zremrangebyrank", "key", "11", "22"),
			want: ZRemRangeByRank{key: "key", start: 11, stop: 22},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZRemRangeByRank)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.start, test.want.start)
				testx.AssertEqual(t, cm.stop, test.want.stop)
			}
		})
	}
}

func TestZRemRangeByRankExec(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "2nd", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)

		cmd := mustParse[*ZRemRangeByRank]("zremrangebyrank key 1 2")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.out(), "2")

		count, _ := db.ZSet().Len("key")
		testx.AssertEqual(t, count, 2)

		one, _ := db.ZSet().GetScore("key", "one")
		testx.AssertEqual(t, one, 1.0)
		_, err = db.ZSet().GetScore("key", "two")
		testx.AssertErr(t, err, core.ErrNotFound)
		_, err = db.ZSet().GetScore("key", "2nd")
		testx.AssertErr(t, err, core.ErrNotFound)
		thr, _ := db.ZSet().GetScore("key", "thr")
		testx.AssertEqual(t, thr, 3.0)
	})
	t.Run("all", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "2nd", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)

		cmd := mustParse[*ZRemRangeByRank]("zremrangebyrank key 0 3")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 4)
		testx.AssertEqual(t, conn.out(), "4")

		count, _ := db.ZSet().Len("key")
		testx.AssertEqual(t, count, 0)
	})
	t.Run("none", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "2nd", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)

		cmd := mustParse[*ZRemRangeByRank]("zremrangebyrank key 4 5")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")

		count, _ := db.ZSet().Len("key")
		testx.AssertEqual(t, count, 4)
	})
	t.Run("negative indexes", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "2nd", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)

		cmd := mustParse[*ZRemRangeByRank]("zremrangebyrank key -2 -1")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")

		count, _ := db.ZSet().Len("key")
		testx.AssertEqual(t, count, 4)
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*ZRemRangeByRank]("zremrangebyrank key 0 3")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = red.Str().Set("key", "str")

		cmd := mustParse[*ZRemRangeByRank]("zremrangebyrank key 0 3")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")

		val, _ := red.Str().Get("key")
		testx.AssertEqual(t, val.String(), "str")
	})
}
