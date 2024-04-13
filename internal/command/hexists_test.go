package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestHExistsParse(t *testing.T) {
	tests := []struct {
		name  string
		args  [][]byte
		key   string
		field string
		err   error
	}{
		{
			name:  "hexists",
			args:  buildArgs("hexists"),
			key:   "",
			field: "",
			err:   ErrInvalidArgNum,
		},
		{
			name:  "hexists person",
			args:  buildArgs("hexists", "person"),
			key:   "",
			field: "",
			err:   ErrInvalidArgNum,
		},
		{
			name:  "hexists person name",
			args:  buildArgs("hexists", "person", "name"),
			key:   "person",
			field: "name",
			err:   nil,
		},
		{
			name:  "hexists person name age",
			args:  buildArgs("hexists", "person", "name", "age"),
			key:   "",
			field: "",
			err:   ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*HExists)
				testx.AssertEqual(t, cm.key, test.key)
				testx.AssertEqual(t, cm.field, test.field)
			}
		})
	}
}

func TestHExistsExec(t *testing.T) {
	t.Run("field found", func(t *testing.T) {
		db, tx := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := mustParse[*HExists]("hexists person name")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, tx)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")
	})
	t.Run("field not found", func(t *testing.T) {
		db, tx := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := mustParse[*HExists]("hexists person age")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, tx)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, tx := getDB(t)
		defer db.Close()

		cmd := mustParse[*HExists]("hexists person name")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, tx)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.out(), "0")
	})
}
