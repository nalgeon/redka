package conn_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/conn"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestEchoParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want []string
		err  error
	}{
		{
			name: "echo",
			args: command.BuildArgs("echo"),
			want: []string{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "echo hello",
			args: command.BuildArgs("echo", "hello"),
			want: []string{"hello"},
			err:  nil,
		},
		{
			name: "echo one two",
			args: command.BuildArgs("echo", "one", "two"),
			want: []string{"one", "two"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*conn.Echo).Parts, test.want)
			}
		})
	}
}

func TestEchoExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	tests := []struct {
		name string
		cmd  *conn.Echo
		res  any
		out  string
	}{
		{
			name: "echo hello",
			cmd:  command.MustParse[*conn.Echo]("echo hello"),
			res:  "hello",
			out:  "hello",
		},
		{
			name: "echo one two",
			cmd:  command.MustParse[*conn.Echo]("echo one two"),
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
