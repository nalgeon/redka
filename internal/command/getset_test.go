package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/testx"
)

func TestGetSetParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want GetSet
		err  error
	}{
		{
			name: "getset",
			args: buildArgs("getset"),
			want: GetSet{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "getset name",
			args: buildArgs("getset", "name"),
			want: GetSet{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "getset name alice",
			args: buildArgs("getset", "name", "alice"),
			want: GetSet{key: "name", value: []byte("alice")},
			err:  nil,
		},
		{
			name: "getset name alice 60",
			args: buildArgs("getset", "name", "alice", "60"),
			want: GetSet{},
			err:  ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*GetSet)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.value, test.want.value)
			}
		})
	}
}

func TestGetSetExec(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db, tx := getDB(t)
		defer db.Close()

		cmd := mustParse[*GetSet]("getset name alice")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, tx)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value(nil))
		testx.AssertEqual(t, conn.out(), "(nil)")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("update", func(t *testing.T) {
		db, tx := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*GetSet]("getset name bob")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, tx)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value("alice"))
		testx.AssertEqual(t, conn.out(), "alice")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "bob")
	})
}
