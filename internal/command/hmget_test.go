package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHMGetParse(t *testing.T) {
	tests := []struct {
		name   string
		args   [][]byte
		key    string
		fields []string
		err    error
	}{
		{
			name:   "hmget",
			args:   buildArgs("hmget"),
			key:    "",
			fields: nil,
			err:    ErrInvalidArgNum("hmget"),
		},
		{
			name:   "hmget person",
			args:   buildArgs("hmget", "person"),
			key:    "",
			fields: nil,
			err:    ErrInvalidArgNum("hmget"),
		},
		{
			name:   "hmget person name",
			args:   buildArgs("hmget", "person", "name"),
			key:    "person",
			fields: []string{"name"},
			err:    nil,
		},
		{
			name:   "hmget person name age",
			args:   buildArgs("hmget", "person", "name", "age"),
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
				cm := cmd.(*HMGet)
				testx.AssertEqual(t, cm.key, test.key)
				testx.AssertEqual(t, cm.fields, test.fields)
			}
		})
	}
}

func TestHMGetExec(t *testing.T) {
	t.Run("one field", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := mustParse[*HMGet]("hmget person name")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, conn.out(), "1,alice")
		items := res.([]core.Value)
		testx.AssertEqual(t, len(items), 1)
		testx.AssertEqual(t, items[0], core.Value("alice"))
	})
	t.Run("some fields", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)
		_, _ = db.Hash().Set("person", "happy", true)

		cmd := mustParse[*HMGet]("hmget person name happy city")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, conn.out(), "3,alice,1,(nil)")
		items := res.([]core.Value)
		testx.AssertEqual(t, len(items), 3)
		testx.AssertEqual(t, items[0], core.Value("alice"))
		testx.AssertEqual(t, items[1], core.Value("1"))
		testx.AssertEqual(t, items[2], core.Value(nil))
	})
	t.Run("all fields", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := mustParse[*HMGet]("hmget person name age")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, conn.out(), "2,alice,25")
		items := res.([]core.Value)
		testx.AssertEqual(t, len(items), 2)
		testx.AssertEqual(t, items[0], core.Value("alice"))
		testx.AssertEqual(t, items[1], core.Value("25"))
	})
}
