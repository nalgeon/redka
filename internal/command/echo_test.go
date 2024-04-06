package command

import (
	"testing"
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
			err:  ErrInvalidArgNum("echo"),
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
			assertEqual(t, err, test.err)
			if err == nil {
				assertEqual(t, cmd.(*Echo).parts, test.want)
			}
		})
	}
}

func TestEchoExec(t *testing.T) {
	db := getDB(t)
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
			res, err := test.cmd.Run(conn, db)
			assertNoErr(t, err)
			assertEqual(t, res, test.res)
			assertEqual(t, conn.out(), test.out)
		})
	}
}
