package list

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/redis"
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
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.index, test.want.index)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestLIndexExec(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseLIndex, "lindex key 0")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value(nil))
		be.Equal(t, conn.Out(), "(nil)")
	})
	t.Run("single elem", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.List().PushBack("key", "elem")

		cmd := redis.MustParse(ParseLIndex, "lindex key 0")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value("elem"))
		be.Equal(t, conn.Out(), "elem")
	})
	t.Run("multiple elems", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.List().PushBack("key", "one")
		_, _ = red.List().PushBack("key", "two")
		_, _ = red.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLIndex, "lindex key 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value("two"))
		be.Equal(t, conn.Out(), "two")
	})
	t.Run("list index out of bounds", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.List().PushBack("key", "elem")

		cmd := redis.MustParse(ParseLIndex, "lindex key 1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value(nil))
		be.Equal(t, conn.Out(), "(nil)")
	})
	t.Run("negative index", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.List().PushBack("key", "one")
		_, _ = red.List().PushBack("key", "two")
		_, _ = red.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLIndex, "lindex key -2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value("two"))
		be.Equal(t, conn.Out(), "two")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "str")

		cmd := redis.MustParse(ParseLIndex, "lindex key 0")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value(nil))
		be.Equal(t, conn.Out(), "(nil)")
	})
}
