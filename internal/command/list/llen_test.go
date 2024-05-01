package list

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestLLenParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want LLen
		err  error
	}{
		{
			cmd:  "llen",
			want: LLen{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "llen key",
			want: LLen{key: "key"},
			err:  nil,
		},
		{
			cmd:  "llen key other",
			want: LLen{},
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseLLen, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
			}
		})
	}
}

func TestLLenExec(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseLLen, "llen key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("single elem", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "elem")

		cmd := redis.MustParse(ParseLLen, "llen key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")
	})
	t.Run("multiple elems", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLLen, "llen key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 3)
		testx.AssertEqual(t, conn.Out(), "3")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		cmd := redis.MustParse(ParseLLen, "llen key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
