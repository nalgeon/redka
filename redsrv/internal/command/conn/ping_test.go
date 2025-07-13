package conn

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestPingParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want string
		err  error
	}{
		{
			cmd:  "ping",
			want: "",
			err:  nil,
		},
		{
			cmd:  "ping hello",
			want: "hello",
			err:  nil,
		},
		{
			cmd:  "ping one two",
			want: "",
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParsePing, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.message, test.want)
			} else {
				be.Equal(t, cmd, Ping{})
			}
		})
	}
}

func TestPingExec(t *testing.T) {
	red := getRedka(t)

	tests := []struct {
		cmd string
		res any
		out string
	}{
		{
			cmd: "ping",
			res: "PONG",
			out: "PONG",
		},
		{
			cmd: "ping hello",
			res: "hello",
			out: "hello",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParsePing, test.cmd)
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res, test.res)
			be.Equal(t, conn.Out(), test.out)
		})
	}
}
