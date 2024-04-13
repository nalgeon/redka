package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestExistsParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want []string
		err  error
	}{
		{
			name: "exists",
			args: buildArgs("exists"),
			want: nil,
			err:  ErrInvalidArgNum,
		},
		{
			name: "exists name",
			args: buildArgs("exists", "name"),
			want: []string{"name"},
			err:  nil,
		},
		{
			name: "exists name age",
			args: buildArgs("exists", "name", "age"),
			want: []string{"name", "age"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*Exists).keys, test.want)
			}
		})
	}
}

func TestExistsExec(t *testing.T) {
	db, tx := getDB(t)
	defer db.Close()

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 50)
	_ = db.Str().Set("city", "paris")

	tests := []struct {
		name string
		cmd  *Exists
		res  any
		out  string
	}{
		{
			name: "exists one",
			cmd:  mustParse[*Exists]("exists name"),
			res:  1,
			out:  "1",
		},
		{
			name: "exists all",
			cmd:  mustParse[*Exists]("exists name age"),
			res:  2,
			out:  "2",
		},
		{
			name: "exists some",
			cmd:  mustParse[*Exists]("exists name age street"),
			res:  2,
			out:  "2",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := new(fakeConn)
			res, err := test.cmd.Run(conn, tx)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.out(), test.out)
		})
	}
}
