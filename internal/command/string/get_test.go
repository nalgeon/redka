package string

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

func TestGetParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want string
		err  error
	}{
		{
			cmd:  "get",
			want: "",
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "get name",
			want: "name",
			err:  nil,
		},
		{
			cmd:  "get name age",
			want: "",
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseGet, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want)
			} else {
				be.Equal(t, cmd, Get{})
			}
		})
	}
}

func TestGetExec(t *testing.T) {
	red := getRedka(t)
	_ = red.Str().Set("name", "alice")

	tests := []struct {
		cmd string
		res any
		out string
	}{
		{
			cmd: "get name",
			res: core.Value("alice"),
			out: "alice",
		},
		{
			cmd: "get age",
			res: core.Value(nil),
			out: "(nil)",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParseGet, test.cmd)
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res, test.res)
			be.Equal(t, conn.Out(), test.out)
		})
	}
}
