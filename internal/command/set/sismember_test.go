package set

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
)

func TestSIsMemberParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SIsMember
		err  error
	}{
		{
			cmd:  "sismember",
			want: SIsMember{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sismember key",
			want: SIsMember{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sismember key one",
			want: SIsMember{key: "key", member: []byte("one")},
			err:  nil,
		},
		{
			cmd:  "sismember key one two",
			want: SIsMember{},
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSIsMember, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.member, test.want.member)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestSIsMemberExec(t *testing.T) {
	t.Run("elem found", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key", "one")

		cmd := redis.MustParse(ParseSIsMember, "sismember key one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "1")
	})
	t.Run("elem not found", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key", "one")

		cmd := redis.MustParse(ParseSIsMember, "sismember key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, false)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseSIsMember, "sismember key one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, false)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "one")

		cmd := redis.MustParse(ParseSIsMember, "sismember key one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, false)
		be.Equal(t, conn.Out(), "0")
	})
}
