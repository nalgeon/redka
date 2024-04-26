package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRemParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZRem
		err  error
	}{
		{
			name: "zrem",
			args: buildArgs("zrem"),
			want: ZRem{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrem key",
			args: buildArgs("zrem", "key"),
			want: ZRem{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrem key member",
			args: buildArgs("zrem", "key", "member"),
			want: ZRem{key: "key", members: []any{"member"}},
			err:  nil,
		},
		{
			name: "zrem key one two thr",
			args: buildArgs("zrem", "key", "one", "two", "thr"),
			want: ZRem{key: "key", members: []any{"one", "two", "thr"}},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZRem)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.members, test.want.members)
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

		cmd := mustParse[*ZRem]("zrem key one two")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.out(), "2")

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

		cmd := mustParse[*ZRem]("zrem key one two thr")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 3)
		testx.AssertEqual(t, conn.out(), "3")

		count, _ := db.ZSet().Len("key")
		testx.AssertEqual(t, count, 0)
	})
	t.Run("none", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_, _ = db.ZSet().Add("key", "two", 2)
		_, _ = db.ZSet().Add("key", "thr", 3)

		cmd := mustParse[*ZRem]("zrem key fou fiv")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")

		count, _ := db.ZSet().Len("key")
		testx.AssertEqual(t, count, 3)
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*ZRem]("zrem key one two")
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

		cmd := mustParse[*ZRem]("zrem key fou fiv")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")

		val, _ := red.Str().Get("key")
		testx.AssertEqual(t, val.String(), "str")
	})
}
