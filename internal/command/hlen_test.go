package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestHLenParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		key  string
		err  error
	}{
		{
			name: "hlen",
			args: buildArgs("hlen"),
			key:  "",
			err:  ErrInvalidArgNum,
		},
		{
			name: "hlen person",
			args: buildArgs("hlen", "person"),
			key:  "person",
			err:  nil,
		},
		{
			name: "hlen person name",
			args: buildArgs("hlen", "person", "name"),
			key:  "",
			err:  ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*HLen)
				testx.AssertEqual(t, cm.key, test.key)
			}
		})
	}
}

func TestHLenExec(t *testing.T) {
	t.Run("key found", func(t *testing.T) {
		db, tx := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := mustParse[*HLen]("hlen person")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, tx)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.out(), "2")
	})
	t.Run("key not found", func(t *testing.T) {
		db, tx := getDB(t)
		defer db.Close()

		cmd := mustParse[*HLen]("hlen person")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, tx)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
}
