package list

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestLPushParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want LPush
		err  error
	}{
		{
			cmd:  "lpush",
			want: LPush{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "lpush key",
			want: LPush{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "lpush key elem",
			want: LPush{key: "key", elem: []byte("elem")},
			err:  nil,
		},
		{
			cmd:  "lpush key elem other",
			want: LPush{},
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseLPush, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.elem, test.want.elem)
			}
		})
	}
}

func TestLPushExec(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseLPush, "lpush key elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 1)
		be.Equal(t, conn.Out(), "1")

		elem, _ := red.List().Get("key", 0)
		be.Equal(t, elem.String(), "elem")
	})
	t.Run("add elem", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.List().PushBack("key", "one")

		cmd := redis.MustParse(ParseLPush, "lpush key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		elem, _ := red.List().Get("key", 0)
		be.Equal(t, elem.String(), "two")
	})
	t.Run("add miltiple", func(t *testing.T) {
		red := getRedka(t)

		{
			cmd := redis.MustParse(ParseLPush, "lpush key one")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res, 1)
			be.Equal(t, conn.Out(), "1")
		}
		{
			cmd := redis.MustParse(ParseLPush, "lpush key two")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res, 2)
			be.Equal(t, conn.Out(), "2")
		}

		el0, _ := red.List().Get("key", 0)
		be.Equal(t, el0.String(), "two")
		el1, _ := red.List().Get("key", 1)
		be.Equal(t, el1.String(), "one")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "str")

		cmd := redis.MustParse(ParseLPush, "lpush key elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), core.ErrKeyType.Error()+" (lpush)")
	})
}
