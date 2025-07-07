package list

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
)

func TestLRemParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want LRem
		err  error
	}{
		{
			cmd:  "lrem",
			want: LRem{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "lrem key",
			want: LRem{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "lrem key elem",
			want: LRem{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "lrem key elem 5",
			want: LRem{},
			err:  redis.ErrInvalidInt,
		},
		{
			cmd:  "lrem key 5 elem",
			want: LRem{key: "key", count: 5, elem: []byte("elem")},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseLRem, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.count, test.want.count)
				be.Equal(t, cmd.elem, test.want.elem)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestLRemExec(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseLRem, "lrem key 1 elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("delete elem", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "elem")

		cmd := redis.MustParse(ParseLRem, "lrem key 1 elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 1)
		be.Equal(t, conn.Out(), "1")

		count, _ := db.List().Len("key")
		be.Equal(t, count, 0)
	})
	t.Run("delete front", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "fou")

		cmd := redis.MustParse(ParseLRem, "lrem key 2 two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		count, _ := db.List().Len("key")
		be.Equal(t, count, 4)
		el1, _ := db.List().Get("key", 1)
		be.Equal(t, el1.String(), "thr")
	})
	t.Run("delete back", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "fou")

		cmd := redis.MustParse(ParseLRem, "lrem key -2 two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		count, _ := db.List().Len("key")
		be.Equal(t, count, 4)
		el1, _ := db.List().Get("key", 1)
		be.Equal(t, el1.String(), "two")
	})
	t.Run("delete all", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "fou")

		cmd := redis.MustParse(ParseLRem, "lrem key 0 two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 3)
		be.Equal(t, conn.Out(), "3")

		count, _ := db.List().Len("key")
		be.Equal(t, count, 3)
	})
	t.Run("elem not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "elem")

		cmd := redis.MustParse(ParseLRem, "lrem key 1 other")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		count, _ := db.List().Len("key")
		be.Equal(t, count, 1)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		cmd := redis.MustParse(ParseLRem, "lrem key 1 elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
}
