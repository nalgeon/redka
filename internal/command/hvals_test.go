package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHValsParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		key  string
		err  error
	}{
		{
			name: "hvals",
			args: buildArgs("hvals"),
			key:  "",
			err:  ErrInvalidArgNum,
		},
		{
			name: "hvals person",
			args: buildArgs("hvals", "person"),
			key:  "person",
			err:  nil,
		},
		{
			name: "hvals person name",
			args: buildArgs("hvals", "person", "name"),
			key:  "",
			err:  ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*HVals)
				testx.AssertEqual(t, cm.key, test.key)
			}
		})
	}
}

func TestHValsExec(t *testing.T) {
	t.Run("key found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := mustParse[*HVals]("hvals person")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []core.Value{core.Value("25"), core.Value("alice")})
		testx.AssertEqual(t, conn.out(), "2,25,alice")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*HVals]("hvals person")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []core.Value{})
		testx.AssertEqual(t, conn.out(), "0")
	})
}
