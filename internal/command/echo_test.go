package command

import (
	"testing"

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
			args: buildArgs("echo"),
			want: []string{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "echo hello",
			args: buildArgs("echo", "hello"),
			want: []string{"hello"},
			err:  nil,
		},
		{
			name: "echo one two",
			args: buildArgs("echo", "one", "two"),
			want: []string{"one", "two"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*Echo).parts, test.want)
			}
		})
	}
}

func TestEchoExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	tests := []struct {
		name string
		cmd  *Echo
		res  any
		out  string
	}{
		{
			name: "echo hello",
			cmd:  mustParse[*Echo]("echo hello"),
			res:  "hello",
			out:  "hello",
		},
		{
			name: "echo one two",
			cmd:  mustParse[*Echo]("echo one two"),
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
