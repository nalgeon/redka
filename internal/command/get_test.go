package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/testx"
)

func TestGetParse(t *testing.T) {
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
			err:  ErrInvalidArgNum,
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
			err:  ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*Get).key, test.want)
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
			res:  core.Value("alice"),
			out:  "alice",
		},
		{
			name: "get not found",
			cmd:  mustParse[*Get]("get age"),
			res:  core.Value(nil),
			out:  "(nil)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := new(fakeConn)
			res, err := test.cmd.Run(conn, db)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.out(), test.out)
		})
	}
}
