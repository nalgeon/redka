package set

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestSRemParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SRem
		err  error
	}{
		{
			cmd:  "srem",
			want: SRem{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "srem key",
			want: SRem{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "srem key one",
			want: SRem{key: "key", members: []any{"one"}},
			err:  nil,
		},
		{
			cmd:  "srem key one two",
			want: SRem{key: "key", members: []any{"one", "two"}},
			err:  nil,
		},
		{
			cmd:  "srem key one two thr",
			want: SRem{key: "key", members: []any{"one", "two", "thr"}},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSRem, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.members, test.want.members)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestSRemExec(t *testing.T) {
	t.Run("some", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key", "one", "two", "thr")

		cmd := redis.MustParse(ParseSRem, "srem key one thr")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		items, _ := red.Set().Items("key")
		be.Equal(t, items, []core.Value{core.Value("two")})
	})
	t.Run("none", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key", "one", "two", "thr")

		cmd := redis.MustParse(ParseSRem, "srem key fou fiv")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		slen, _ := red.Set().Len("key")
		be.Equal(t, slen, 3)
	})
	t.Run("key not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseSRem, "srem key one two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "str")

		cmd := redis.MustParse(ParseSRem, "srem key one two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
}
