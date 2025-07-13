package list

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestLInsertParse(t *testing.T) {
	tests := []struct {
		cmd   string
		want  LInsert
		index int
		err   error
	}{
		{
			cmd:  "linsert",
			want: LInsert{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "linsert key",
			want: LInsert{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "linsert key before",
			want: LInsert{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "linsert key before pivot",
			want: LInsert{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "linsert key before pivot elem",
			want: LInsert{key: "key", where: Before, pivot: []byte("pivot"), elem: []byte("elem")},
			err:  nil,
		},
		{
			cmd:  "linsert key after pivot elem",
			want: LInsert{key: "key", where: After, pivot: []byte("pivot"), elem: []byte("elem")},
			err:  nil,
		},
		{
			cmd:  "linsert key inplace pivot elem",
			want: LInsert{},
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseLInsert, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.where, test.want.where)
				be.Equal(t, cmd.pivot, test.want.pivot)
				be.Equal(t, cmd.elem, test.want.elem)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestLInsertExec(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseLInsert, "linsert key before pivot elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		_, err = red.List().Get("key", 0)
		be.Equal(t, err, core.ErrNotFound)
	})
	t.Run("insert before first", func(t *testing.T) {
		red := getRedka(t)
		_, err := red.List().PushBack("key", "pivot")
		be.Err(t, err, nil)

		cmd := redis.MustParse(ParseLInsert, "linsert key before pivot elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		elem, _ := red.List().Get("key", 0)
		be.Equal(t, elem, core.Value("elem"))
	})
	t.Run("insert before middle", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.List().PushBack("key", "one")
		_, _ = red.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLInsert, "linsert key before thr two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 3)
		be.Equal(t, conn.Out(), "3")

		elem, _ := red.List().Get("key", 1)
		be.Equal(t, elem, core.Value("two"))
	})
	t.Run("insert after middle", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.List().PushBack("key", "one")
		_, _ = red.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLInsert, "linsert key after thr two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 3)
		be.Equal(t, conn.Out(), "3")

		elem, _ := red.List().Get("key", 2)
		be.Equal(t, elem, core.Value("two"))
	})
	t.Run("elem not found", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.List().PushBack("key", "one")
		_, _ = red.List().PushBack("key", "two")

		cmd := redis.MustParse(ParseLInsert, "linsert key before thr two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, -1)
		be.Equal(t, conn.Out(), "-1")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "str")

		cmd := redis.MustParse(ParseLInsert, "linsert key before pivot elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
}
