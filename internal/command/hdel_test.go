package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestHDelParse(t *testing.T) {
	tests := []struct {
		name   string
		args   [][]byte
		key    string
		fields []string
		err    error
	}{
		{
			name:   "hdel",
			args:   buildArgs("hdel"),
			key:    "",
			fields: nil,
			err:    ErrInvalidArgNum,
		},
		{
			name:   "hdel person",
			args:   buildArgs("hdel", "person"),
			key:    "",
			fields: nil,
			err:    ErrInvalidArgNum,
		},
		{
			name:   "hdel person name",
			args:   buildArgs("hdel", "person", "name"),
			key:    "person",
			fields: []string{"name"},
			err:    nil,
		},
		{
			name:   "hdel person name age",
			args:   buildArgs("hdel", "person", "name", "age"),
			key:    "person",
			fields: []string{"name", "age"},
			err:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*HDel)
				testx.AssertEqual(t, cm.key, test.key)
				testx.AssertEqual(t, cm.fields, test.fields)
			}
		})
	}
}

func TestHDelExec(t *testing.T) {
	t.Run("one", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := mustParse[*HDel]("hdel person name")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.out(), "1")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.Exists(), false)
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "25")
	})
	t.Run("some", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)
		_, _ = db.Hash().Set("person", "happy", true)

		cmd := mustParse[*HDel]("hdel person name happy city")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.out(), "2")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.Exists(), false)
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "25")
		happy, _ := db.Hash().Get("person", "happy")
		testx.AssertEqual(t, happy.Exists(), false)
	})
	t.Run("all", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := mustParse[*HDel]("hdel person name age")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.out(), "2")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.Exists(), false)
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.Exists(), false)
	})
}
