package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestDelParse(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name string
		args [][]byte
		want []string
		err  error
	}{
		{
			name: "del",
			args: buildArgs("del"),
			want: nil,
			err:  ErrInvalidArgNum("del"),
		},
		{
			name: "del name",
			args: buildArgs("del", "name"),
			want: []string{"name"},
			err:  nil,
		},
		{
			name: "del name age",
			args: buildArgs("del", "name", "age"),
			want: []string{"name", "age"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*Del).keys, test.want)
			}
		})
	}
}

func TestDelExec(t *testing.T) {
	tests := []struct {
		name string
		cmd  *Del
		res  any
		out  string
	}{
		{
			name: "del one",
			cmd:  mustParse[*Del]("del name"),
			res:  1,
			out:  "1",
		},
		{
			name: "del all",
			cmd:  mustParse[*Del]("del name age"),
			res:  2,
			out:  "2",
		},
		{
			name: "del some",
			cmd:  mustParse[*Del]("del name age street"),
			res:  2,
			out:  "2",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := getDB(t)
			defer db.Close()

			_ = db.Str().Set("name", "alice")
			_ = db.Str().Set("age", 50)
			_ = db.Str().Set("city", "paris")

			conn := new(fakeConn)
			res, err := test.cmd.Run(conn, db)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.out(), test.out)

			name, _ := db.Str().Get("name")
			testx.AssertEqual(t, name.IsEmpty(), true)
			city, _ := db.Str().Get("city")
			testx.AssertEqual(t, city.String(), "paris")
		})
	}
}
