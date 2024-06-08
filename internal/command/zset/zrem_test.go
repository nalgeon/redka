package zset

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRemParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZRem
		err  error
	}{
		{
			cmd:  "zrem",
			want: ZRem{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrem key",
			want: ZRem{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrem key member",
			want: ZRem{key: "key", members: []any{"member"}},
			err:  nil,
		},
		{
			cmd:  "zrem key one two thr",
			want: ZRem{key: "key", members: []any{"one", "two", "thr"}},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZRem, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.members, test.want.members)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestZRemExec(t *testing.T) {
	t.Run("some", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)

		cmd := redis.MustParse(ParseZRem, "zrem key one two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "2")

		count, _ := db.ZSet().Len("key")
		testx.AssertEqual(t, count, 1)

		_, err = db.ZSet().GetScore("key", "one")
		testx.AssertErr(t, err, core.ErrNotFound)
		_, err = db.ZSet().GetScore("key", "two")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
	t.Run("all", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)

		cmd := redis.MustParse(ParseZRem, "zrem key one two thr")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 3)
		testx.AssertEqual(t, conn.Out(), "3")

		count, _ := db.ZSet().Len("key")
		testx.AssertEqual(t, count, 0)
	})
	t.Run("none", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)

		cmd := redis.MustParse(ParseZRem, "zrem key fou fiv")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		count, _ := db.ZSet().Len("key")
		testx.AssertEqual(t, count, 3)
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseZRem, "zrem key one two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = red.Str().Set("key", "str")

		cmd := redis.MustParse(ParseZRem, "zrem key fou fiv")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		val, _ := red.Str().Get("key")
		testx.AssertEqual(t, val.String(), "str")
	})
}
