package command

import (
	"testing"

	"github.com/nalgeon/redka"
)

func TestGetParse(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name string
		args [][]byte
		want string
		err  error
	}{
		{
			name: "get",
			args: buildArgs("get"),
			want: "",
			err:  ErrInvalidArgNum("get"),
		},
		{
			name: "get name",
			args: buildArgs("get", "name"),
			want: "name",
			err:  nil,
		},
		{
			name: "get name age",
			args: buildArgs("get", "name", "age"),
			want: "",
			err:  ErrInvalidArgNum("get"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			assertEqual(t, err, test.err)
			if err == nil {
				assertEqual(t, cmd.(*Get).key, test.want)
			}
		})
	}
}

func TestGetExec(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	_ = db.Str().Set("name", "alice")

	tests := []struct {
		name string
		cmd  *Get
		res  any
		out  string
	}{
		{
			name: "get found",
			cmd:  mustParse[*Get]("get name"),
			res:  redka.Value("alice"),
			out:  "alice",
		},
		{
			name: "get not found",
			cmd:  mustParse[*Get]("get age"),
			res:  redka.Value(nil),
			out:  "(nil)",
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
