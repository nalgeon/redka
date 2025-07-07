package list

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

func TestLSetParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want LSet
		err  error
	}{
		{
			cmd:  "lset",
			want: LSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "lset key",
			want: LSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "lset key elem",
			want: LSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "lset key elem 5",
			want: LSet{},
			err:  redis.ErrInvalidInt,
		},
		{
			cmd:  "lset key 5 elem",
			want: LSet{key: "key", index: 5, elem: []byte("elem")},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseLSet, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.index, test.want.index)
				be.Equal(t, cmd.elem, test.want.elem)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestLSetExec(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseLSet, "lset key 0 elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, core.ErrNotFound)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), redis.ErrOutOfRange.Error()+" (lset)")
	})
	t.Run("set elem", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLSet, "lset key 1 upd")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), "OK")

		el1, _ := db.List().Get("key", 1)
		be.Equal(t, el1.String(), "upd")
	})
	t.Run("negative index", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLSet, "lset key -1 upd")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), "OK")

		el2, _ := db.List().Get("key", 2)
		be.Equal(t, el2.String(), "upd")
	})
	t.Run("index out of bounds", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "elem")

		cmd := redis.MustParse(ParseLSet, "lset key 1 upd")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, core.ErrNotFound)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), redis.ErrOutOfRange.Error()+" (lset)")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		cmd := redis.MustParse(ParseLSet, "lset key 0 elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, core.ErrNotFound)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), redis.ErrOutOfRange.Error()+" (lset)")
	})
}
