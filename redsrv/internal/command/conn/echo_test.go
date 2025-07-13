package conn

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestEchoParse(t *testing.T) {
	tests := []struct {
		cmd  string
		args [][]byte
		want []string
		err  error
	}{
		{
			cmd:  "echo",
			want: []string{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "echo hello",
			want: []string{"hello"},
			err:  nil,
		},
		{
			cmd:  "echo one two",
			want: []string{"one", "two"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseEcho, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.parts, test.want)
			} else {
				be.Equal(t, cmd, Echo{})
			}
		})
	}
}

func TestEchoExec(t *testing.T) {
	red := getRedka(t)

	tests := []struct {
		cmd string
		res any
		out string
	}{
		{
			cmd: "echo hello",
			res: "hello",
			out: "hello",
		},
		{
			cmd: "echo one two",
			res: "one two",
			out: "one two",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParseEcho, test.cmd)
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res, test.res)
			be.Equal(t, conn.Out(), test.out)
		})
	}
}
