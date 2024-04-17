package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestHKeysParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		key  string
		err  error
	}{
		{
			name: "hkeys",
			args: buildArgs("hkeys"),
			key:  "",
			err:  ErrInvalidArgNum,
		},
		{
			name: "hkeys person",
			args: buildArgs("hkeys", "person"),
			key:  "person",
			err:  nil,
		},
		{
			name: "hkeys person name",
			args: buildArgs("hkeys", "person", "name"),
			key:  "",
			err:  ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*HKeys)
				testx.AssertEqual(t, cm.key, test.key)
			}
		})
	}
}

func TestHKeysExec(t *testing.T) {
	t.Run("key found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := mustParse[*HKeys]("hkeys person")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []string{"age", "name"})
		testx.AssertEqual(t,
			conn.out() == "2,age,name" || conn.out() == "2,name,age",
			true)
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*HKeys]("hkeys person")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []string{})
		testx.AssertEqual(t, conn.out(), "0")
	})
}
