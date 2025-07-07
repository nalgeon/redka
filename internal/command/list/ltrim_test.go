package list

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
)

func TestLTrimParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want LTrim
		err  error
	}{
		{
			cmd:  "ltrim",
			want: LTrim{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "ltrim key 1",
			want: LTrim{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "ltrim key 1 -2",
			want: LTrim{key: "key", start: 1, stop: -2},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseLTrim, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.start, test.want.start)
				be.Equal(t, cmd.stop, test.want.stop)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestLTrimExec(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseLTrim, "ltrim key 0 0")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "OK")
	})
	t.Run("keep single elem", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "elem")

		cmd := redis.MustParse(ParseLTrim, "ltrim key 0 0")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "OK")

		count, _ := db.List().Len("key")
		be.Equal(t, count, 1)
	})
	t.Run("keep multiple elems", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")
		_, _ = db.List().PushBack("key", "fou")

		cmd := redis.MustParse(ParseLTrim, "ltrim key 1 2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "OK")

		count, _ := db.List().Len("key")
		be.Equal(t, count, 2)
		el0, _ := db.List().Get("key", 0)
		be.Equal(t, el0.String(), "two")
		el1, _ := db.List().Get("key", 1)
		be.Equal(t, el1.String(), "thr")
	})
	t.Run("negative index", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")
		_, _ = db.List().PushBack("key", "fou")

		cmd := redis.MustParse(ParseLTrim, "ltrim key 0 -1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "OK")

		count, _ := db.List().Len("key")
		be.Equal(t, count, 4)
	})
	t.Run("start > stop", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLTrim, "ltrim key 2 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 3)
		be.Equal(t, conn.Out(), "OK")

		count, _ := db.List().Len("key")
		be.Equal(t, count, 0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		cmd := redis.MustParse(ParseLTrim, "ltrim key 0 0")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "OK")
	})
}
