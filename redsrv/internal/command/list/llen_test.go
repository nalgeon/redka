package list

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/redsrv/internal/redis"
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
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestLLenExec(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseLLen, "llen key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("single elem", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.List().PushBack("key", "elem")

		cmd := redis.MustParse(ParseLLen, "llen key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 1)
		be.Equal(t, conn.Out(), "1")
	})
	t.Run("multiple elems", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.List().PushBack("key", "one")
		_, _ = red.List().PushBack("key", "two")
		_, _ = red.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLLen, "llen key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 3)
		be.Equal(t, conn.Out(), "3")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "str")

		cmd := redis.MustParse(ParseLLen, "llen key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
}
