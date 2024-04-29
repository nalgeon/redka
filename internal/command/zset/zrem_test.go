package zset_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/zset"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRemParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want zset.ZRem
		err  error
	}{
		{
			name: "zrem",
			args: command.BuildArgs("zrem"),
			want: zset.ZRem{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zrem key",
			args: command.BuildArgs("zrem", "key"),
			want: zset.ZRem{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zrem key member",
			args: command.BuildArgs("zrem", "key", "member"),
			want: zset.ZRem{Key: "key", Members: []any{"member"}},
			err:  nil,
		},
		{
			name: "zrem key one two thr",
			args: command.BuildArgs("zrem", "key", "one", "two", "thr"),
			want: zset.ZRem{Key: "key", Members: []any{"one", "two", "thr"}},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*zset.ZRem)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Members, test.want.Members)
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

		cmd := command.MustParse[*zset.ZRem]("zrem key one two")
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

		cmd := command.MustParse[*zset.ZRem]("zrem key one two thr")
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

		cmd := command.MustParse[*zset.ZRem]("zrem key fou fiv")
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

		cmd := command.MustParse[*zset.ZRem]("zrem key one two")
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

		cmd := command.MustParse[*zset.ZRem]("zrem key fou fiv")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		val, _ := red.Str().Get("key")
		testx.AssertEqual(t, val.String(), "str")
	})
}
