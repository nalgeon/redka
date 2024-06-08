package conn

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
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
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.message, test.want)
			} else {
				testx.AssertEqual(t, cmd, Ping{})
			}
		})
	}
}

func TestPingExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

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
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.Out(), test.out)
		})
	}
}
