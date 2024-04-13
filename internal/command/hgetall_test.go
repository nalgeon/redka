package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHGetAllParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		key  string
		err  error
	}{
		{
			name: "hgetall",
			args: buildArgs("hgetall"),
			key:  "",
			err:  ErrInvalidArgNum("hgetall"),
		},
		{
			name: "hgetall person",
			args: buildArgs("hgetall", "person"),
			key:  "person",
			err:  nil,
		},
		{
			name: "hgetall person name",
			args: buildArgs("hgetall", "person", "name"),
			key:  "",
			err:  ErrInvalidArgNum("hgetall"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*HGetAll)
				testx.AssertEqual(t, cm.key, test.key)
			}
		})
	}
}

func TestHGetAllExec(t *testing.T) {
	t.Run("key found", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := mustParse[*HGetAll]("hgetall person")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, map[string]core.Value{
			"name": core.Value("alice"), "age": core.Value("25"),
		})
		testx.AssertEqual(t, conn.out(), "4,name,alice,age,25")
	})
	t.Run("key not found", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		cmd := mustParse[*HGetAll]("hgetall person")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, map[string]core.Value{})
		testx.AssertEqual(t, conn.out(), "0")
	})
}
