package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestAppendParse(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name string
		args [][]byte
		want Append
		err  error
	}{
		{
			name: "append",
			args: buildArgs("append"),
			want: Append{},
			err:  ErrInvalidArgNum("append"),
		},
		{
			name: "append name",
			args: buildArgs("append", "name"),
			want: Append{},
			err:  ErrInvalidArgNum("append"),
		},
		{
			name: "append name bob",
			args: buildArgs("append", "name", "bob"),
			want: Append{key: "name", value: "bob"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*Append)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.value, test.want.value)
			}
		})
	}
}

func TestAppendExec(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	t.Run("create", func(t *testing.T) {
		cmd := mustParse[*Append]("append name alice")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 5)
		testx.AssertEqual(t, conn.out(), "5")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("append", func(t *testing.T) {
		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*Append]("append name bob")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 8)
		testx.AssertEqual(t, conn.out(), "8")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alicebob")
	})
}
