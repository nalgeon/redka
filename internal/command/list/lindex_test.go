package list

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestLIndexParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want LIndex
		err  error
	}{
		{
			cmd:  "lindex",
			want: LIndex{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "lindex key",
			want: LIndex{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "lindex key 2",
			want: LIndex{key: "key", index: 2},
			err:  nil,
		},
		{
			cmd:  "lindex key 2 3",
			want: LIndex{},
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseLIndex, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.index, test.want.index)
			}
		})
	}
}

func TestLIndexExec(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseLIndex, "lindex key 0")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
	t.Run("single elem", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "elem")

		cmd := redis.MustParse(ParseLIndex, "lindex key 0")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value("elem"))
		testx.AssertEqual(t, conn.Out(), "elem")
	})
	t.Run("multiple elems", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLIndex, "lindex key 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value("two"))
		testx.AssertEqual(t, conn.Out(), "two")
	})
	t.Run("list index out of bounds", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "elem")

		cmd := redis.MustParse(ParseLIndex, "lindex key 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
	t.Run("negative index", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLIndex, "lindex key -2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value("two"))
		testx.AssertEqual(t, conn.Out(), "two")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		cmd := redis.MustParse(ParseLIndex, "lindex key 0")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
}
