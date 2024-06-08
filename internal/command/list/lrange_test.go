package list

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestLRangeParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want LRange
		err  error
	}{
		{
			cmd:  "lrange",
			want: LRange{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "lrange key",
			want: LRange{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "lrange key 0",
			want: LRange{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "lrange key 0 -1",
			want: LRange{key: "key", start: 0, stop: -1},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseLRange, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.start, test.want.start)
				testx.AssertEqual(t, cmd.stop, test.want.stop)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestLRangeExec(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseLRange, "lrange key 0 0")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("single elem", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "elem")

		cmd := redis.MustParse(ParseLRange, "lrange key 0 0")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []core.Value{core.Value("elem")})
		testx.AssertEqual(t, conn.Out(), "1,elem")
	})
	t.Run("multiple elems", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLRange, "lrange key 0 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []core.Value{core.Value("one"), core.Value("two")})
		testx.AssertEqual(t, conn.Out(), "2,one,two")
	})
	t.Run("negative indexes", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLRange, "lrange key -2 -1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []core.Value{core.Value("two"), core.Value("thr")})
		testx.AssertEqual(t, conn.Out(), "2,two,thr")
	})
	t.Run("out of bounds", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLRange, "lrange key 3 5")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("start < stop", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLRange, "lrange key 2 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		cmd := redis.MustParse(ParseLRange, "lrange key 0 0")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
