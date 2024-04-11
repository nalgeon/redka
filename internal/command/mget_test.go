package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/testx"
)

func TestMGetParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want []string
		err  error
	}{
		{
			name: "mget",
			args: buildArgs("mget"),
			want: nil,
			err:  ErrInvalidArgNum("mget"),
		},
		{
			name: "mget name",
			args: buildArgs("mget", "name"),
			want: []string{"name"},
			err:  nil,
		},
		{
			name: "mget name age",
			args: buildArgs("mget", "name", "age"),
			want: []string{"name", "age"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*MGet)
				testx.AssertEqual(t, cm.keys, test.want)
			}
		})
	}
}

func TestMGetExec(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 25)

	tests := []struct {
		name string
		cmd  *MGet
		res  any
		out  string
	}{
		{
			name: "single key",
			cmd:  mustParse[*MGet]("mget name"),
			res:  []core.Value{core.Value("alice")},
			out:  "1,alice",
		},
		{
			name: "multiple keys",
			cmd:  mustParse[*MGet]("mget name age"),
			res:  []core.Value{core.Value("alice"), core.Value("25")},
			out:  "2,alice,25",
		},
		{
			name: "some not found",
			cmd:  mustParse[*MGet]("mget name city age"),
			res:  []core.Value{core.Value("alice"), core.Value(nil), core.Value("25")},
			out:  "3,alice,(nil),25",
		},
		{
			name: "all not found",
			cmd:  mustParse[*MGet]("mget one two"),
			res:  []core.Value{core.Value(nil), core.Value(nil)},
			out:  "2,(nil),(nil)",
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
