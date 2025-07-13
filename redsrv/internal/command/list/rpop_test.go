package list

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestRPopParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want RPop
		err  error
	}{
		{
			cmd:  "rpop",
			want: RPop{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "rpop key",
			want: RPop{key: "key"},
			err:  nil,
		},
		{
			cmd:  "rpop key 5",
			want: RPop{},
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseRPop, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestRPopExec(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseRPop, "rpop key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value(nil))
		be.Equal(t, conn.Out(), "(nil)")
	})
	t.Run("pop elem", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.List().PushBack("key", "elem")

		cmd := redis.MustParse(ParseRPop, "rpop key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value("elem"))
		be.Equal(t, conn.Out(), "elem")
	})
	t.Run("pop multiple", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.List().PushBack("key", "one")
		_, _ = red.List().PushBack("key", "two")
		_, _ = red.List().PushBack("key", "thr")

		{
			cmd := redis.MustParse(ParseRPop, "rpop key")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res.(core.Value), core.Value("thr"))
			be.Equal(t, conn.Out(), "thr")
		}
		{
			cmd := redis.MustParse(ParseRPop, "rpop key")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res.(core.Value), core.Value("two"))
			be.Equal(t, conn.Out(), "two")
		}
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "str")

		cmd := redis.MustParse(ParseRPop, "rpop key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value(nil))
		be.Equal(t, conn.Out(), "(nil)")
	})
}
