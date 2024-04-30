package command

import (
	"testing"

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
			args: buildArgs("ping"),
			want: []string{"PONG"},
			err:  nil,
		},
		{
			name: "ping hello",
			args: buildArgs("ping", "hello"),
			want: []string{"hello"},
			err:  nil,
		},
		{
			name: "ping one two",
			args: buildArgs("ping", "one", "two"),
			want: []string{"one", "two"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*Ping).parts, test.want)
			}
		})
	}
}

func TestPingExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	tests := []struct {
		name string
		cmd  *Ping
		res  any
		out  string
	}{
		{
			name: "ping",
			cmd:  mustParse[*Ping]("ping"),
			res:  "PONG",
			out:  "PONG",
		},
		{
			name: "ping hello",
			cmd:  mustParse[*Ping]("ping hello"),
			res:  "hello",
			out:  "hello",
		},
		{
			name: "ping one two",
			cmd:  mustParse[*Ping]("ping one two"),
			res:  "one two",
			out:  "one two",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := new(fakeConn)
			res, err := test.cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.out(), test.out)
		})
	}
}
