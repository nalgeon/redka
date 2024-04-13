package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHGetParse(t *testing.T) {
	tests := []struct {
		name  string
		args  [][]byte
		key   string
		field string
		err   error
	}{
		{
			name:  "hget",
			args:  buildArgs("hget"),
			key:   "",
			field: "",
			err:   ErrInvalidArgNum,
		},
		{
			name:  "hget person",
			args:  buildArgs("hget", "person"),
			key:   "",
			field: "",
			err:   ErrInvalidArgNum,
		},
		{
			name:  "hget person name",
			args:  buildArgs("hget", "person", "name"),
			key:   "person",
			field: "name",
			err:   nil,
		},
		{
			name:  "hget person name age",
			args:  buildArgs("hget", "person", "name", "age"),
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
				cm := cmd.(*HGet)
				testx.AssertEqual(t, cm.key, test.key)
				testx.AssertEqual(t, cm.field, test.field)
			}
		})
	}
}

func TestHGetExec(t *testing.T) {
	t.Run("field found", func(t *testing.T) {
		db, tx := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := mustParse[*HGet]("hget person name")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, tx)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value("alice"))
		testx.AssertEqual(t, conn.out(), "alice")
	})
	t.Run("field not found", func(t *testing.T) {
		db, tx := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := mustParse[*HGet]("hget person age")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, tx)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value(nil))
		testx.AssertEqual(t, conn.out(), "(nil)")
	})
	t.Run("key not found", func(t *testing.T) {
		db, tx := getDB(t)
		defer db.Close()

		cmd := mustParse[*HGet]("hget person name")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, tx)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value(nil))
		testx.AssertEqual(t, conn.out(), "(nil)")
	})
}
