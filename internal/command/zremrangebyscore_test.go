package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRemRangeByScoreParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZRemRangeByScore
		err  error
	}{
		{
			name: "zremrangebyscore",
			args: buildArgs("zremrangebyscore"),
			want: ZRemRangeByScore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zremrangebyscore key",
			args: buildArgs("zremrangebyscore", "key"),
			want: ZRemRangeByScore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zremrangebyscore key 1.1",
			args: buildArgs("zremrangebyscore", "key", "1.1"),
			want: ZRemRangeByScore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zremrangebyscore key 1.1 2.2",
			args: buildArgs("zremrangebyscore", "key", "1.1", "2.2"),
			want: ZRemRangeByScore{key: "key", min: 1.1, max: 2.2},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZRemRangeByScore)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.min, test.want.min)
				testx.AssertEqual(t, cm.max, test.want.max)
			}
		})
	}
}

func TestZRemRangeByScoreExec(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 10)
		_, _ = db.ZSet().Add("key", "two", 20)
		_, _ = db.ZSet().Add("key", "2nd", 20)
		_, _ = db.ZSet().Add("key", "thr", 30)

		cmd := mustParse[*ZRemRangeByScore]("zremrangebyscore key 10 20")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 3)
		testx.AssertEqual(t, conn.out(), "3")

		count, _ := db.ZSet().Len("key")
		testx.AssertEqual(t, count, 1)

		_, err = db.ZSet().GetScore("key", "one")
		testx.AssertErr(t, err, core.ErrNotFound)
		_, err = db.ZSet().GetScore("key", "two")
		testx.AssertErr(t, err, core.ErrNotFound)
		_, err = db.ZSet().GetScore("key", "2nd")
		testx.AssertErr(t, err, core.ErrNotFound)
		thr, _ := db.ZSet().GetScore("key", "thr")
		testx.AssertEqual(t, thr, 30.0)
	})
	t.Run("all", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 10)
		_, _ = db.ZSet().Add("key", "two", 20)
		_, _ = db.ZSet().Add("key", "2nd", 20)
		_, _ = db.ZSet().Add("key", "thr", 30)

		cmd := mustParse[*ZRemRangeByScore]("zremrangebyscore key 0 30")
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
		_, _ = db.ZSet().Add("key", "one", 10)
		_, _ = db.ZSet().Add("key", "two", 20)
		_, _ = db.ZSet().Add("key", "2nd", 20)
		_, _ = db.ZSet().Add("key", "thr", 30)

		cmd := mustParse[*ZRemRangeByScore]("zremrangebyscore key 40 50")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")

		count, _ := db.ZSet().Len("key")
		testx.AssertEqual(t, count, 4)
	})
	t.Run("negative scores", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", -10)
		_, _ = db.ZSet().Add("key", "two", -20)
		_, _ = db.ZSet().Add("key", "2nd", -20)
		_, _ = db.ZSet().Add("key", "thr", -30)

		cmd := mustParse[*ZRemRangeByScore]("zremrangebyscore key -20 -10")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 3)
		testx.AssertEqual(t, conn.out(), "3")

		count, _ := db.ZSet().Len("key")
		testx.AssertEqual(t, count, 1)
		thr, _ := db.ZSet().GetScore("key", "thr")
		testx.AssertEqual(t, thr, -30.0)
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*ZRemRangeByScore]("zremrangebyscore key 0 30")
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

		cmd := mustParse[*ZRemRangeByScore]("zremrangebyscore key 0 30")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")

		val, _ := red.Str().Get("key")
		testx.AssertEqual(t, val.String(), "str")
	})
}
