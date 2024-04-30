package conn_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/conn"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestPingParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want []string
		err  error
	}{
		{
			name: "ping",
			args: command.BuildArgs("ping"),
			want: []string(nil),
			err:  nil,
		},
		{
			name: "ping hello",
			args: command.BuildArgs("ping", "hello"),
			want: []string{"hello"},
			err:  nil,
		},
		{
			name: "ping one two",
			args: command.BuildArgs("ping", "one", "two"),
			want: []string{"one", "two"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*conn.Ping).Parts, test.want)
			}
		})
	}
}

func TestPingExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	tests := []struct {
		name string
		cmd  *conn.Ping
		res  any
		out  string
	}{
		{
			name: "ping",
			cmd:  command.MustParse[*conn.Ping]("ping"),
			res:  "PONG",
			out:  "PONG",
		},
		{
			name: "ping hello",
			cmd:  command.MustParse[*conn.Ping]("ping hello"),
			res:  "hello",
			out:  "hello",
		},
		{
			name: "ping one two",
			cmd:  command.MustParse[*conn.Ping]("ping one two"),
			res:  "one two",
			out:  "one two",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := redis.NewFakeConn()
			res, err := test.cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.Out(), test.out)
		})
	}
}
