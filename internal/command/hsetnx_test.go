package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestHSetNXParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want HSetNX
		err  error
	}{
		{
			name: "hsetnx",
			args: buildArgs("hsetnx"),
			want: HSetNX{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "hsetnx person",
			args: buildArgs("hsetnx", "person"),
			want: HSetNX{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "hsetnx person name",
			args: buildArgs("hsetnx", "person", "name"),
			want: HSetNX{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "hsetnx person name alice",
			args: buildArgs("hsetnx", "person", "name", "alice"),
			want: HSetNX{key: "person", field: "name", value: []byte("alice")},
			err:  nil,
		},
		{
			name: "hsetnx person name alice age 25",
			args: buildArgs("hsetnx", "person", "name", "alice", "age", "25"),
			want: HSetNX{},
			err:  ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*HSetNX)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.value, test.want.value)
			}
		})
	}
}

func TestHSetNXExec(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		cmd := mustParse[*HSetNX]("hsetnx person name alice")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("update", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := mustParse[*HSetNX]("hsetnx person name bob")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.out(), "0")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
	})
}
