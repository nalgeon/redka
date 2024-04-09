package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestFlushDBParse(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name string
		args [][]byte
		err  error
	}{
		{
			name: "flushdb",
			args: buildArgs("flushdb"),
			err:  nil,
		},
		{
			name: "flushdb name",
			args: buildArgs("flushdb", "name"),
			err:  ErrSyntaxError,
		},
		{
			name: "flushdb 1",
			args: buildArgs("flushdb", "1"),
			err:  ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
		})
	}
}

func TestFlushDBExec(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		cmd := mustParse[*FlushDB]("flushdb")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "OK")

		keys, _ := db.Key().Search("*")
		testx.AssertEqual(t, len(keys), 0)
	})

	t.Run("empty", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		cmd := mustParse[*FlushDB]("flushdb")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "OK")

		keys, _ := db.Key().Search("*")
		testx.AssertEqual(t, len(keys), 0)
	})
}
